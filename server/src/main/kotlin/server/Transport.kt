package io.github.akmal2409.nats.server.server

import io.github.akmal2409.nats.server.common.closeAndIgnore
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
import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
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

private data class ClientChannelMetadata<IN>(
    val clientKey: ClientKey,
    val readBuffer: ByteBuffer,
    var deserializer: RequestDeserializer<IN>?
)

private class ClientMessage(val bytes: ByteBuffer, val clientKey: ClientKey)
private class ClientBuffer(val queue: Queue<ByteBuffer>, var totalSize: Int)

private class ResponseWriterLoop<IN, OUT>(
    val registry: ClientRegistry<IN, OUT>,
    val messageBytesQueue: ConcurrentLinkedQueue<ClientMessage>,
    val maxProcessingCount: Int = 100,
    val maxConsumerBufferSize: Int = 10 * 1024 * 1024 // 10MB
) {

    private val clientQueuedMessages = HashMap<ClientKey, ClientBuffer>()

    fun run() {

        val selector = Selector.open()

        while (!Thread.currentThread().isInterrupted && selector.isOpen) {
            val selectedKeys = selector.selectedKeys()

            if (selectedKeys.size < maxProcessingCount) {
                val queuedMessage = try {
                    messageBytesQueue.remove()
                } catch (ex: NoSuchElementException) {
                    null
                }

                queuedMessage?.let {
                    val client = registry[queuedMessage.clientKey] ?: return@let

                    val existingBuffer = clientQueuedMessages.containsKey(queuedMessage.clientKey)
                    val clientBufferSize = clientQueuedMessages[it.clientKey]?.totalSize ?: 0

                    if (clientBufferSize > maxConsumerBufferSize) {
                        // TODO: Kill slow consumer, add threshold to notify first
                    } else {
                        logger.info { "Adding message to buffer for client ${queuedMessage.clientKey}" }

                        val clientBuffer =
                            clientQueuedMessages[queuedMessage.clientKey] ?: ClientBuffer(
                                LinkedList(),
                                0
                            )


                        clientBuffer.queue.offer(queuedMessage.bytes)
                        clientBuffer.totalSize += queuedMessage.bytes.capacity()

                        if (!existingBuffer) {
                            logger.info { "Registering client for write" }
                            // register for writing
                            val bytesRead = client.socketChannel.write(queuedMessage.bytes)

                            if (!queuedMessage.bytes.hasRemaining()) {

                                clientBuffer.totalSize -= bytesRead
                                client.socketChannel.register(selector, SelectionKey.OP_WRITE)
                                    .attach(client.key)
                            }
                        }
                    }
                }
            }

            for (selectedKey in selectedKeys) {
                logger.info { "Channel is ready to be written to" }
                val clientKey = selectedKey.attachment() as ClientKey? ?: continue
                val channel = selectedKey.channel() as SocketChannel
                val clientBuffer = clientQueuedMessages[clientKey]

                if (clientBuffer == null || clientBuffer.queue.isEmpty()) {
                    if (clientBuffer?.queue?.isEmpty() == true) {
                        clientQueuedMessages.remove(clientKey)
                    }
                    selectedKey.cancel()
                } else if (clientBuffer.queue.size == 1 && !clientBuffer.queue.peek()
                        .hasRemaining()
                ) {
                    clientQueuedMessages.remove(clientKey)
                    selectedKey.cancel()
                } else {
                    val byteBuffer = clientBuffer.queue.peek()

                    // TODO: error handling
                    channel.write(byteBuffer)

                    if (!byteBuffer.hasRemaining()) {
                        clientBuffer.queue.poll()
                        clientBuffer.totalSize -= byteBuffer.capacity()

                        if (clientBuffer.queue.isEmpty()) {
                            clientQueuedMessages.remove(clientKey)
                        }
                    }
                }
            }
        }

        logger.info { "Shutting down writer" }
    }

}

private fun <K> clientErrorDefaultHandler(): (K, SelectionKey, Throwable) -> Unit = { _, _, _ ->
}

/**
 * Class that handles logic of accepting new clients and reading from client socket
 * when its available
 */
class ClientHandler<IN, OUT>(
    private val registry: ClientRegistry<IN, OUT>,
    private val bufferFactory: () -> ByteBuffer,
    private val connectionHandler: ConnectionHandler<IN, OUT>,
    private val coroutineScope: CoroutineScope,
    private val onResponse: suspend (ClientKey, OUT) -> Unit,
    private val onParsed: (ClientKey, IN) -> Unit,
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
            client.key, bufferFactory(), null
        )

        responseChannel.consumeAsFlow()
            .onEach { response -> onResponse(client.key, response) }
            .catch { ex ->
                onResponseError(client.key, selectionKey, ex)

            }.launchIn(coroutineScope)

        clientChannel.register(readSelector, SelectionKey.OP_READ)
            .attach(metadata)

        logger.info { "Accepted connection from ${clientChannel.remoteAddress}" }

        registry[client.key] = client
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

                    clientMeta.readBuffer.clear()


                    when (partialResult) {
                        is DeserializationResult.AvailableResult<IN> -> {
                            onParsed(clientMeta.clientKey, partialResult.value)
                            clientMeta.deserializer = requestDeserializerFactory()
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

        val writer = ResponseWriterLoop<IN, OUT>(registry, messageByteQueue)
        writeLoopThread.set(
            thread(
                start = true,
                isDaemon = true,
                name = "NIO-Writer",
                block = writer::run
            )
        )

        val clientHandler = ClientHandler<IN, OUT>(
            registry = registry,
            connectionHandler = connectionHandler, // binding that is provided by the client of the transport
            // it receives complete parsed requests and return responses
            coroutineScope = coroutineScope,
            bufferFactory = { ByteBuffer.allocateDirect(bufferSize) },
            onParsed = ::onRequestParsed,
            onResponse = ::onResponse,
            requestDeserializerFactory = requestDeserializerFactory,
            onRequestError = ::onRequestError,
            onResponseError = ::onResponseError
        )

        while (!Thread.currentThread().isInterrupted && selector.isOpen) {
            try {
                selector.select { selectionKey ->
                    if (selectionKey.isAcceptable) {
                        clientHandler.onAccept(selectionKey, selector)
                    } else if (selectionKey.isReadable) {
                        clientHandler.onRead(selectionKey)
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
            client.responseChannel.cancel(CancellationException(cause))
            clientMeta.deserializer?.close()
            client.socketChannel.closeAndIgnore()
        }
    }
}