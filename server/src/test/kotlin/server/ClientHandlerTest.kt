package server

import io.github.akmal2409.nats.server.common.remainingAsString
import io.github.akmal2409.nats.server.server.ClientChannelMetadata
import io.github.akmal2409.nats.server.server.ClientHandler
import io.github.akmal2409.nats.server.server.ClientKey
import io.github.akmal2409.nats.server.server.ClientRegistry
import io.github.akmal2409.nats.server.server.ConnectionHandler
import io.github.akmal2409.nats.server.server.DeserializationResult
import io.github.akmal2409.nats.server.server.RequestDeserializer
import io.kotest.assertions.withClue
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.types.shouldBeTypeOf
import io.mockk.every
import io.mockk.mockk
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestScope
import kotlin.test.BeforeTest
import kotlin.test.Test

fun interface MockRequestDeserializer<OUT> : RequestDeserializer<OUT> {

    override fun close() {}
}

class ClientHandlerTest {

    lateinit var coroutineScheduler: TestCoroutineScheduler
    lateinit var testScope: TestScope

    val requestDeserializer = mockk<RequestDeserializer<String>>()
    val clientRegistry = mockk<ClientRegistry<String, String>>()

    val bufferFactory = { ByteBuffer.allocate(1024) }

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

        requestBuffer.position(0).limit(7).remainingAsString(Charsets.US_ASCII) shouldBeEqual "Request"
        deserInvoked.shouldBeTrue()
    }

    private fun newHandler(
        overrideRegistry: ClientRegistry<String, String> = clientRegistry,
        overrideBufferFactory: () -> ByteBuffer = bufferFactory,
        overrideConnectionHandler: ConnectionHandler<String, String> = emptyConnectionHandler,
        overrideOnResponse: suspend (ClientKey, String) -> Unit = noResponseHandler,
        overrideOnParsed: (ClientKey, String) -> Unit = noParsedHandler,
        overrideRequestDeserFactory: () -> RequestDeserializer<String> = { requestDeserializer },
        overrideOnRequestError: (ClientKey?, SelectionKey, Throwable) -> Unit = noRequestErrorHandler,
        overrideOnResponseError: (ClientKey, SelectionKey, Throwable) -> Unit = noResponseErrorHandler
    ) = ClientHandler<String, String>(
        overrideRegistry, overrideBufferFactory, overrideConnectionHandler,
        testScope, overrideOnResponse, overrideOnParsed, overrideRequestDeserFactory,
        overrideOnRequestError, overrideOnResponseError
    )
}
