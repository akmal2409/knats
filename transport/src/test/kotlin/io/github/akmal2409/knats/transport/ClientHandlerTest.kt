package io.github.akmal2409.knats.transport

import io.github.akmal2409.knats.extensions.remainingAsString
import io.kotest.assertions.withClue
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.types.shouldBeTypeOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.time.Duration
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.time.toKotlinDuration

fun interface MockRequestDeserializer<OUT> : RequestDeserializer<OUT> {

    override fun close() {}
}

private const val DEFAULT_BUFF_SIZE = 1024

class ClientHandlerTest {

    lateinit var coroutineScheduler: TestCoroutineScheduler
    lateinit var testScope: TestScope

    val requestDeserializer = mockk<RequestDeserializer<String>>()
    val clientRegistry = mockk<ClientRegistry<String, String>>()

    val bufferFactory = { ByteBuffer.allocate(DEFAULT_BUFF_SIZE) }

    val noResponseErrorHandler: (ClientKey, SelectionKey, Throwable) -> Unit =
        { clientKey, selection, ex ->
            unexpectedInvocationError(
                "responseErrorHandler",
                clientKey,
                selection,
                ex
            )
        }

    val noRequestErrorHandler: (ClientKey?, SelectionKey, Throwable) -> Unit =
        { clientKey, selection, ex ->
            unexpectedInvocationError(
                "requestErrorHandler",
                clientKey,
                selection,
                ex
            )
        }

    val noParsedHandler: (ClientKey, String) -> Unit =
        { clientKey, parsed -> unexpectedInvocationError("parsedHandler", clientKey, parsed) }

    val noResponseHandler: suspend (ClientKey, String) -> Unit =
        { clientKey, response -> unexpectedInvocationError("responseHandler", clientKey, response) }

    val emptyConnectionHandler = ConnectionHandler<String, String> { emptyFlow() }

    val noClientRegisteredHandler: (Client<String, String>) -> Unit =
        { unexpectedInvocationError("clientRegisteredHandler") }

    private fun unexpectedInvocationError(method: String, vararg args: Any?) {
        val argsJoined = args.joinToString(separator = ",", prefix = "args=[", postfix = "]")

        error("Invoked $method but not expected with args $argsJoined")
    }

    @BeforeTest
    fun setUp() {
        coroutineScheduler = TestCoroutineScheduler()
        testScope = TestScope(coroutineScheduler)
    }

    @Test
    fun `Reports error when client metadata is not present`() {
        val mockSelectionKey = mockk<SelectionKey>()

        var requestErrorInvoked = false

        val requestErrorHandler: (ClientKey?, SelectionKey, Throwable) -> Unit =
            { clientKey, selectionKey, ex ->
                clientKey.shouldBeNull()
                selectionKey shouldBeEqual mockSelectionKey
                ex.shouldBeTypeOf<IllegalStateException>()
                requestErrorInvoked = true
            }

        val handler = newHandler(overrideOnRequestError = requestErrorHandler)

        every { mockSelectionKey.attachment() } returns null
        every { mockSelectionKey.channel() } returns mockk<SocketChannel>()

        handler.onRead(mockSelectionKey)

        withClue("RequestErrorHandler was not invoked") {
            requestErrorInvoked.shouldBeTrue()
        }
    }

    @Test
    fun `Handles error when parsing throws`() {
        val testClientKey = ClientKey.fromRemoteAddress("/localhost")
        val parseException = IllegalArgumentException("Parsing error")

        var requestErrorInvoked = false
        val requestErrorHandler: (ClientKey?, SelectionKey, Throwable) -> Unit =
            { clientKey, selection, ex ->
                clientKey.shouldNotBeNull()
                clientKey shouldBeEqual testClientKey

                ex shouldBeEqual parseException

                requestErrorInvoked = true
            }

        val clientHandler = newHandler(overrideOnRequestError = requestErrorHandler)
        val selectionKey = mockk<SelectionKey>()
        val channel = mockk<SocketChannel>()

        val readBuffer = ByteBuffer.wrap(ByteArray(10))

        every { selectionKey.attachment() } returns ClientChannelMetadata<String>(
            testClientKey,
            readBuffer
        )
        every { selectionKey.channel() } returns channel
        every { channel.read(readBuffer) } returns 10

        every { requestDeserializer.deserialize(readBuffer) } throws parseException

        clientHandler.onRead(selectionKey)

        requestErrorInvoked.shouldBeTrue()
    }

    @Test
    fun `Parses request when provided`() {
        val clientKey = ClientKey.fromRemoteAddress("/localhost")

        val request = "Test Request"
        var deserInvoked = false

        val requestBuffer = ByteBuffer.wrap(request.encodeToByteArray())

        val requestDeser = MockRequestDeserializer { buffer ->
            buffer.array().decodeToString() shouldBeEqual request

            deserInvoked = true
            DeserializationResult.of(request)
        }

        val meta = ClientChannelMetadata(clientKey, requestBuffer, requestDeser)

        var parsedInvoked = false
        val onParsed: (ClientKey, String) -> Unit = { key, parsedRequest ->
            parsedRequest shouldBeEqual request
            key shouldBeEqual clientKey
            parsedInvoked = true
        }

        val handler = newHandler(overrideOnParsed = onParsed)

        val selection = mockk<SelectionKey>()
        val channel = mockk<SocketChannel>()

        every { selection.attachment() } returns meta
        every { selection.channel() } returns channel
        every { channel.read(requestBuffer) } returns requestBuffer.limit()

        handler.onRead(selection)

        deserInvoked.shouldBeTrue()
        parsedInvoked.shouldBeTrue()
    }

    @Test
    fun `If bytes are left after parsing, copies them to the start of the buffer`() {
        val clientKey = ClientKey.fromRemoteAddress("/localhost")

        val request = "Test Request"
        var deserInvoked = false

        val encodedRequest = request.toByteArray(Charsets.US_ASCII)
        val requestBuffer = ByteBuffer.wrap(encodedRequest)

        val requestDeser = MockRequestDeserializer { buffer ->

            buffer.limit(buffer.capacity())
            buffer.position(request.split(" ")[0].length + 1)

            deserInvoked = true
            DeserializationResult.of(request)
        }

        val handler = newHandler(overrideOnParsed = { _, _ -> },
            overrideRequestDeserFactory = { requestDeser })

        val selectionKey = mockk<SelectionKey>()
        val channel = mockk<SocketChannel>()
        every { selectionKey.attachment() } returns ClientChannelMetadata<String>(
            clientKey,
            requestBuffer
        )
        every { selectionKey.channel() } returns channel
        every { channel.read(requestBuffer) } returns encodedRequest.size

        handler.onRead(selectionKey)

        requestBuffer.limit() shouldBeExactly encodedRequest.size

        withClue("Read buffer position should be: ") {
            requestBuffer.position() shouldBeExactly 7
        }

        requestBuffer.position(0).limit(7)
            .remainingAsString(Charsets.US_ASCII) shouldBeEqual "Request"
        deserInvoked.shouldBeTrue()
    }

    @Test
    fun `Successfully registers the client`() {
        val clientSocketAddress = mockk<InetSocketAddress>()
        val clientAddress = Inet4Address.getByName("10.0.0.1")

        val socketChannel = mockk<SocketChannel>()
        val clientSelectionKey = mockk<SelectionKey>()
        val serverSocketChannel = mockk<ServerSocketChannel>()

        val responseFlow = emptyFlow<String>()
        val selectionKey = mockk<SelectionKey>()
        val selector = mockk<Selector>()

        var clientRegisteredInvoked = false
        var connectionHandlerInvoked = false

        val connectionHandler = ConnectionHandler<String, String> { request ->
            request.remoteAddress shouldBeEqual clientAddress
            request.requestFlow.shouldNotBeNull()
            connectionHandlerInvoked = true
            responseFlow
        }


        val onClientRegistered: (Client<String, String>) -> Unit = { client ->
            client.key.remoteAddress shouldBeEqual clientAddress.toString()
            client.key.id.shouldNotBeNull()
            client.socketChannel shouldBeEqual socketChannel
            client.requestChannel.shouldNotBeNull()
            client.responseChannel.shouldNotBeNull()
            client.bufferedMessageCount.get() shouldBeEqual 0
            clientRegisteredInvoked = true
        }

        val clientHandler = newHandler(
            overrideOnClientRegistered = onClientRegistered,
            overrideConnectionHandler = connectionHandler
        )

        val capturedClientMetadata = slot<ClientChannelMetadata<String>>()
        val capturedConfigureBlocking = slot<Boolean>()

        every { selectionKey.channel() } returns serverSocketChannel
        every { serverSocketChannel.accept() } returns socketChannel
        every { socketChannel.remoteAddress } returns clientSocketAddress
        every { clientSocketAddress.address } returns clientAddress

        every { socketChannel.register(selector, SelectionKey.OP_READ) } returns clientSelectionKey

        every { clientSelectionKey.attach(capture(capturedClientMetadata)) } answers {
            capturedClientMetadata.isCaptured.shouldBeTrue()
            val metadata = capturedClientMetadata.captured
            metadata.clientKey.remoteAddress shouldBeEqual clientAddress.toString()
            metadata.clientKey.id.shouldNotBeNull()
            metadata.deserializer.shouldBeNull()
            metadata.readBuffer.capacity() shouldBeExactly DEFAULT_BUFF_SIZE
            null
        }

        every { socketChannel.configureBlocking(capture(capturedConfigureBlocking)) } answers {
            capturedConfigureBlocking.captured.shouldBeFalse()

            mockk()
        }

        clientHandler.onAccept(selectionKey, selector)

        verify(exactly = 1) { socketChannel.register(selector, SelectionKey.OP_READ) }

        withClue("ClientRegistered handler not invoked") {
            clientRegisteredInvoked.shouldBeTrue()
        }

        withClue("ConnectionHandler not invoked") {
            connectionHandlerInvoked.shouldBeTrue()
        }
    }

    @Test
    fun `Request flow transmits data to connection handler`() = testScope
        .runTest(timeout = Duration.ofSeconds(1).toKotlinDuration()) {
            val selectionKey = mockClientConnected()

            val requests = listOf("First", "Second")
            var requestFlow: Flow<String>? = null
            var requestChannel: Channel<String>? = null


            val connectionHandler = ConnectionHandler<String, String> { request ->
                requestFlow = request.requestFlow

                emptyFlow<String>()
            }

            val onClientRegistered: (Client<String, String>) -> Unit = { client ->
                requestChannel = client.requestChannel
            }

            val handler = newHandler(
                overrideConnectionHandler = connectionHandler,
                overrideOnClientRegistered = onClientRegistered
            )

            handler.onAccept(selectionKey, mockk())

            requestFlow.shouldNotBeNull()
            requestChannel.shouldNotBeNull()

            launch {
                for (request in requests) requestChannel!!.send(request)
                requestChannel!!.close()
            }

            requestFlow!!.toList() shouldBeEqual requests
        }

    @Test
    fun `Response flow transmits data from connection handler`() = testScope.runTest {
        val selectionKey = mockClientConnected()


        val responseFlow = flowOf("First", "Second")
        val actualResponses = mutableListOf<String>()

        val connectionHandler = ConnectionHandler<String, String> { responseFlow }
        val onResponseHandler: suspend (ClientKey, String) -> Unit = { clientKey, response ->
            clientKey.shouldNotBeNull()
            clientKey.remoteAddress.shouldNotBeNull()
            clientKey.id.shouldNotBeNull()

            actualResponses.add(response)
        }

        var responseChannel: ReceiveChannel<String>? = null

        val handler = newHandler(
            overrideConnectionHandler = connectionHandler,
            overrideOnResponse = onResponseHandler,
            overrideOnClientRegistered = { client ->
                responseChannel = client.responseChannel
            }
        )

        handler.onAccept(selectionKey, mockk())

        delay(1000)
        actualResponses shouldBeEqual listOf("First", "Second")
    }

    private fun mockClientConnected(): SelectionKey {
        val selectionKey = mockk<SelectionKey>()
        val serverSocketChannel = mockk<ServerSocketChannel>()
        val clientSocketChannel = mockk<SocketChannel>()
        val remoteAddress = InetSocketAddress(InetAddress.getByName("10.0.0.1"), 1001)


        every { selectionKey.channel() } returns serverSocketChannel
        every { serverSocketChannel.accept() } returns clientSocketChannel
        every { clientSocketChannel.remoteAddress } returns remoteAddress
        val clientSelectionKey = mockk<SelectionKey>()
        every { clientSocketChannel.register(any(), any()) } returns clientSelectionKey
        every { clientSelectionKey.attach(any()) } returns mockk()
        every { clientSocketChannel.configureBlocking(any()) } returns mockk()

        return selectionKey
    }

    private fun newHandler(
        overrideBufferFactory: () -> ByteBuffer = bufferFactory,
        overrideConnectionHandler: ConnectionHandler<String, String> = emptyConnectionHandler,
        overrideOnResponse: suspend (ClientKey, String) -> Unit = noResponseHandler,
        overrideOnParsed: (ClientKey, String) -> Unit = noParsedHandler,
        overrideOnClientRegistered: (Client<String, String>) -> Unit = noClientRegisteredHandler,
        overrideRequestDeserFactory: () -> RequestDeserializer<String> = { requestDeserializer },
        overrideOnRequestError: (ClientKey?, SelectionKey, Throwable) -> Unit = noRequestErrorHandler,
        overrideOnResponseError: (ClientKey, SelectionKey, Throwable) -> Unit = noResponseErrorHandler
    ) = ClientHandler<String, String>(
        overrideBufferFactory,
        overrideConnectionHandler,
        testScope,
        overrideOnResponse,
        overrideOnParsed,
        overrideOnClientRegistered,
        overrideRequestDeserFactory,
        overrideOnRequestError,
        overrideOnResponseError
    )
}
