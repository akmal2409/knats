package io.github.akmal2409.knats.transport

import io.github.akmal2409.knats.extensions.closeAndIgnore
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ClosedSelectorException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlin.concurrent.thread
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException

data class ClientRequest<T>(
    val requestFlow: Flow<T>,
    val remoteAddress: InetAddress
)

private val logger = KotlinLogging.logger {}

internal data class ClientChannelMetadata<IN>(
    val clientKey: ClientKey,
    val readBuffer: ByteBuffer,
    var deserializer: RequestDeserializer<IN>? = null
)

internal class ClientMessage(val bytes: ByteBuffer, val clientKey: ClientKey)
internal class ClientBuffer(
    val queue: Queue<ByteBuffer>,
    var totalSize: Int = 0,
    var droppedMessages: Int = 0
)

internal class ResponseWriter<IN, OUT>(
    private val clientQueuedMessages: MutableMap<ClientKey, ClientBuffer>,
    private val messageBytesQueue: ConcurrentLinkedQueue<ClientMessage>,
    private val registry: ClientRegistry<IN, OUT>,
    private val maxInFlightMessages: Int = 1000,
    private val maxConsumerBufferSize: Int = 10 * 1024 * 1024
) {

    private var currentInFlightMessages = 0

    /**
     * Method should be invoked in a loop that processes 1 message from a queue and
     * attempts to send other queued messages
     */
    fun sendResponses() {

        while (currentInFlightMessages < maxInFlightMessages && messageBytesQueue.isNotEmpty()) {
            pollNextResponse()
        }

        val dropClients = mutableListOf<ClientKey>()
        val sendToClients = mutableListOf<Client<IN, OUT>>()

        for ((clientKey, buffer) in clientQueuedMessages) {

            val client = registry[clientKey]

            if (client == null) {
                dropClients.add(clientKey)
            } else {
                sendToClients.add(client)
            }
        }

        sendToClients.forEach { sendResponseToClient(it.socketChannel, it.key) }

        dropClients.forEach { clientKey ->
            val droppedBuffer = clientQueuedMessages.remove(clientKey)
            currentInFlightMessages -= droppedBuffer?.queue?.size ?: 0

            logger.debug {
                "Dropping client buffer of size ${droppedBuffer?.totalSize} because " +
                        "no entry in registry for key $clientKey"
            }
        }

    }

    /**
     * Takes messages from response queue and enqueues byte buffers for clients
     */
    private fun pollNextResponse() {
        val queuedMessage: ClientMessage? = messageBytesQueue.poll()

        queuedMessage?.let {
            currentInFlightMessages++
            val clientBuffer = clientQueuedMessages[it.clientKey]
                ?: ClientBuffer(ArrayDeque())

            val clientBufferSize = clientBuffer.totalSize

            if (clientBufferSize > maxConsumerBufferSize) {
                // TODO: Kill slow consumer, add threshold to notify first
                logger.warn { "Dropping message for consumer client key=${it.clientKey} because buffer is overflown" }

                clientBuffer.droppedMessages++
            } else {
                logger.info { "Adding message to buffer for client ${queuedMessage.clientKey}" }

                clientBuffer.queue.offer(queuedMessage.bytes)
                clientBuffer.totalSize += queuedMessage.bytes.remaining()

                clientQueuedMessages[queuedMessage.clientKey] = clientBuffer
            }
        }
    }

    private fun sendResponseToClient(channel: SocketChannel, clientKey: ClientKey) {
        val clientBuffer = clientQueuedMessages[clientKey]

        clientBuffer?.let {
            val byteBuffer = clientBuffer.queue.peek()

            // TODO: error handling
            val bytesLeft = byteBuffer.remaining()
            val bytesWritten = channel.write(byteBuffer)
            val actualBytesWritten =
                bytesLeft - byteBuffer.remaining() // we can't trust .write() result

            clientBuffer.totalSize -= actualBytesWritten

            if (actualBytesWritten > 0) {
                logger.info { "Written $actualBytesWritten to the client key=$clientKey" }
            }

            if (bytesWritten == 0) {
                logger.info {
                    "System buffer is full will attempt to send again remaining " +
                            "bytes remaining=${clientBuffer.totalSize} to client key $clientKey"
                }
            }

            if (!byteBuffer.hasRemaining()) {
                clientBuffer.queue.poll()

                currentInFlightMessages--

                if (clientBuffer.queue.isEmpty()) {
                    clientQueuedMessages.remove(clientKey)
                    logger.info { "Drained whole buffer for client key=$clientKey" }
                }
            }

        }

    }
}

internal class ResponseWriterLoop<IN, OUT>(
    val registry: ClientRegistry<IN, OUT>,
    val messageBytesQueue: ConcurrentLinkedQueue<ClientMessage>,
    val maxInFlightMessages: Int = 1000,
    val maxConsumerBufferSize: Int = 10 * 1024 * 1024 // 10MB
) {

    private val clientQueuedMessages = HashMap<ClientKey, ClientBuffer>()

    fun run() {

        val writer = ResponseWriter(
            clientQueuedMessages, messageBytesQueue, registry, maxInFlightMessages,
            maxConsumerBufferSize
        )

        while (!Thread.currentThread().isInterrupted) {
            writer.sendResponses()
        }

        logger.info { "Shutting down writer" }
    }

}

private fun <K> clientErrorDefaultHandler(): (K, SelectionKey, Throwable) -> Unit = { _, _, _ -> }

/**
 * Class that handles logic of accepting new clients and reading from client socket
 * when its available
 */
internal class ClientHandler<IN, OUT>(
    private val bufferFactory: () -> ByteBuffer,
    private val connectionHandler: ConnectionHandler<IN, OUT>,
    private val coroutineScope: CoroutineScope,
    private val onResponse: suspend (ClientKey, OUT) -> Unit,
    private val onParsed: (ClientKey, IN) -> Unit,
    private val onClientRegistered: (Client<IN, OUT>) -> Unit,
    private val requestDeserializerFactory: () -> RequestDeserializer<IN>,
    private val onRequestError: (ClientKey?, SelectionKey, Throwable) -> Unit = clientErrorDefaultHandler(),
    private val onResponseError: (ClientKey, SelectionKey, Throwable) -> Unit = clientErrorDefaultHandler()
) {

    fun onAccept(selectionKey: SelectionKey, readSelector: Selector) {
        val clientChannel = (selectionKey.channel() as ServerSocketChannel).accept().apply {
            configureBlocking(false)
        }

        val requestChannel = Channel<IN>()
        val remoteAddress = (clientChannel.remoteAddress as? InetSocketAddress)?.address
            ?: error("Could not get clientAddress")


        val responseFlow = connectionHandler.onConnect(
            ClientRequest(
                requestChannel.consumeAsFlow(),
                remoteAddress
            )
        )

        val responseChannel = responseFlow.produceIn(coroutineScope)

        val client = Client(
            ClientKey.fromRemoteAddress(remoteAddress.toString()),
            requestChannel, responseChannel, clientChannel
        )

        val metadata = ClientChannelMetadata<IN>(
            client.key, bufferFactory()
        )

        val readSelectionKey = clientChannel.register(readSelector, SelectionKey.OP_READ).apply {
            attach(metadata)
        }

        responseChannel.consumeAsFlow()
            .onEach { response -> onResponse(client.key, response) }
            .catch { ex ->
                onResponseError(client.key, readSelectionKey, ex)
            }.launchIn(coroutineScope)

        logger.info { "Accepted connection from ${clientChannel.remoteAddress}" }

        onClientRegistered(client)
    }

    @Suppress("UNCHECKED_CAST")
    fun onRead(selectionKey: SelectionKey) {
        val clientMeta = selectionKey.attachment() as? ClientChannelMetadata<IN>
        val channel = selectionKey.channel() as SocketChannel

        if (clientMeta == null) {
            logger.error { "Cancelling selector: No client metadata found ${channel.remoteAddress}" }
            onRequestError(
                clientMeta,
                selectionKey,
                IllegalStateException("No metadata found for client")
            )
        } else {
            if (clientMeta.deserializer == null) {
                clientMeta.deserializer = requestDeserializerFactory()
            }

            val deserializer = clientMeta.deserializer!!

            try {
                val bytesRead = channel.read(clientMeta.readBuffer)

                if (bytesRead > 0) {
                    clientMeta.readBuffer.flip()

                    val partialResult = try {
                        deserializer.deserialize(clientMeta.readBuffer)
                    } catch (ex: Throwable) {
                        logger.error(ex) { "Could not parse request. Removing client" }

                        onRequestError(clientMeta.clientKey, selectionKey, ex)
                        return
                    }

                    if (clientMeta.readBuffer.hasRemaining()) {
                        // wasn't read fully, we need to shift everything what hasn't been read to the start
                        clientMeta.readBuffer.compact()
                    } else {
                        clientMeta.readBuffer.clear()
                    }

                    when (partialResult) {
                        is DeserializationResult.AvailableResult<IN> -> {
                            onParsed(clientMeta.clientKey, partialResult.value)
                            clientMeta.deserializer?.close()
                            clientMeta.deserializer = null
                        }

                        else -> {
                            // continue reading
                        }
                    }

                }
            } catch (ex: IOException) {
                onRequestError(clientMeta.clientKey, selectionKey, ex)
            }
        }
    }
}

class Transport<IN, OUT>(
    private val serverSocketChannelFactory: ServerSocketChannelFactory,
    private val selectorFactory: SelectorFactory,
    private val bufferSize: Int = 1024 * 4,
    private val connectionHandler: ConnectionHandler<IN, OUT>,
    private val requestDeserializerFactory: () -> RequestDeserializer<IN>,
    private val responseSerializer: ResponseSerializer<OUT>,
    private val coroutineContext: CoroutineContext = Dispatchers.Default,
    private val registry: ClientRegistry<IN, OUT> = inMemoryClientRegistry()
) {

    private val running = AtomicReference(false)
    private val started = AtomicBoolean(false)
    private val mainLoopThread = AtomicReference<Thread>(null)
    private val writeLoopThread = AtomicReference<Thread>(null)

    private val messageByteQueue = ConcurrentLinkedQueue<ClientMessage>()

    private val coroutineScope =
        CoroutineScope(coroutineContext) + CoroutineName("NIO-Transport") + SupervisorJob()

    private val globalResponseChannel = Channel<ClientMessage>()

    fun start() {
        if (!running.compareAndSet(false, true) && mainLoopThread.get() == null) {
            error("The server is already running/or has been shutdown")
        }

        val thread = thread(start = true, isDaemon = false, name = "NIO-Server", block = this::run)
        mainLoopThread.set(thread)
    }

    private fun run() {
        val selector = selectorFactory.create()

        val serverSocket = serverSocketChannelFactory.create().apply {
            register(selector, SelectionKey.OP_ACCEPT)
        }

        logger.info { "Starting server listening at port=${serverSocket.localAddress}" }

        coroutineScope.launch {
            responseCollector()
        }

        val writer = ResponseWriterLoop(registry, messageByteQueue)
        writeLoopThread.set(
            thread(
                start = true,
                isDaemon = true,
                name = "NIO-Writer",
                block = writer::run
            )
        )

        val clientHandler = ClientHandler(
            connectionHandler = connectionHandler, // binding that is provided by the client of the transport
            // it receives complete parsed requests and return responses
            coroutineScope = coroutineScope,
            bufferFactory = { ByteBuffer.allocateDirect(bufferSize) },
            onParsed = ::onRequestParsed,
            onResponse = ::onResponse,
            onClientRegistered = { registry[it.key] = it },
            requestDeserializerFactory = requestDeserializerFactory,
            onRequestError = ::onRequestError,
            onResponseError = ::onResponseError
        )

        started.set(true)
        while (!Thread.currentThread().isInterrupted && selector.isOpen) {

            try {
                selector.selectNow { selectionKey ->
                    try {
                        if (selectionKey.isAcceptable) {
                            clientHandler.onAccept(selectionKey, selector)
                        } else if (selectionKey.isReadable) {
                            clientHandler.onRead(selectionKey)
                        }
                    } catch (uncaught: Throwable) {
                        logger.error(uncaught) { "Uncaught error in channels" }
                    }
                }
            } catch (closed: ClosedSelectorException) {
                logger.error(closed) { "Selector is closed with reason $closed" }
            } catch (ioEx: IOException) {
                logger.error(ioEx) { "IOException happened while selecting" }
            }
        }

        logger.info { "Shutting down server" }
    }

    public fun awaitStart(duration: Duration) {
        val stopWaitingAt = Instant.now().plus(duration)

        while (stopWaitingAt.isAfter(Instant.now()) && !started.get()) {
            // busy wait
        }
    }

    /**
     * Whenever the request contents are parsed using provided parser, this method is invoked.
     * The method passes parsed request to the client request channel, which does all the business processing.
     */
    private fun onRequestParsed(clientKey: ClientKey, request: IN) {
        registry[clientKey]?.let { client ->

            coroutineScope.launch {
                client.requestChannel.send(request)
            }

        }
    }

    private fun onRequestError(clientKey: ClientKey?, selectionKey: SelectionKey, ex: Throwable) {
        logger.error(ex) { "Exception occurred when processing request for client $clientKey" }
        selectionKey.cancelClientAndIgnore(ex)
    }

    private fun onResponseError(clientKey: ClientKey, selectionKey: SelectionKey, ex: Throwable) {
        logger.error(ex) { "Exception occurred when processing response for client $clientKey" }
        selectionKey.cancelClientAndIgnore(ex)
    }

    /**
     * Sends response from the connection handler to the global message channel.
     * We have another coroutine that listens to this channel, collects all messages
     * and puts it into a queue so that response writer loop can send responses.
     */
    private suspend fun onResponse(clientKey: ClientKey, response: OUT) {
        globalResponseChannel.send(
            ClientMessage(
                responseSerializer.serialize(response),
                clientKey
            )
        )
    }

    /**
     * Collects messages from the globalResponseChannel and puts them into a queue
     * so that the response loop can consume and write it back to the client.
     */
    private suspend fun responseCollector() {
        logger.info { "Starting response collector " }
        while (true) {
            val clientMessage = globalResponseChannel.receive()
            logger.info { "Forwarding message for ${clientMessage.clientKey}" }
            messageByteQueue.offer(clientMessage)
        }
    }

    fun stop() {
        if (!running.compareAndSet(true, false)) {
            error("Server is not running")
        }
        val thread = mainLoopThread.get()
        writeLoopThread.get().interrupt()
        thread.interrupt()
        coroutineScope.cancel(CancellationException("Shutting down server"))
    }

    /**
     * Removes client, closes channel, cancels selector and clears attachments.
     */
    private fun SelectionKey.cancelClientAndIgnore(cause: Throwable? = null) = try {
        attachment()
            ?.let { clientMeta -> clientMeta as ClientChannelMetadata<IN> }
            ?.let { clientMeta -> removeClient(clientMeta, cause) }
        cancelWithClear()
    } catch (ex: Throwable) {
        // ignored
    }

    /**
     * Clears attachment (client metadata), closes chanel and cancels selector
     */
    private fun SelectionKey.cancelWithClear() {
        attach(null)
        channel().closeAndIgnore()
        cancel()
    }

    private fun removeClient(clientMeta: ClientChannelMetadata<IN>, cause: Throwable? = null) {
        registry.remove(clientMeta.clientKey)?.let { client ->
            logger.info { "Removing client ${clientMeta.clientKey} from registry" }

            client.requestChannel.cancel(CancellationException(cause))
            client.responseChannel.cancel()
            clientMeta.deserializer?.close()
            client.socketChannel.closeAndIgnore()
        }
    }
}
