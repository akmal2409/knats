package transport

import io.github.akmal2409.nats.transport.Client
import io.github.akmal2409.nats.transport.ClientBuffer
import io.github.akmal2409.nats.transport.ClientKey
import io.github.akmal2409.nats.transport.ClientMessage
import io.github.akmal2409.nats.transport.ClientRegistry
import io.github.akmal2409.nats.transport.ResponseWriter
import io.github.akmal2409.nats.transport.common.remainingAsString
import io.github.akmal2409.nats.transport.inMemoryClientRegistry
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.ints.shouldBeZero
import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.maps.shouldNotContainKey
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.ArrayDeque
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.Test

private class TestContext(
    val messageQueue: ConcurrentLinkedQueue<ClientMessage>,
    val registry: ClientRegistry<String, String>,
    val key: ClientKey,
    val clientBuffers: MutableMap<ClientKey, ClientBuffer>,
    val mockSocketChannel: SocketChannel,
    val writer: ResponseWriter<String, String>
)

class ResponseWriterTest {

    @Test
    fun `Sends complete single message to client in 1 go`() {
        val context = initContext()
        context.registry[context.key] = Client(
            context.key, mockk(), mockk(),
            context.mockSocketChannel
        )

        val expectedMessage = "Hello world!";

        val clientMessage = createMessage(expectedMessage, context.key)

        // when
        context.messageQueue.offer(clientMessage)
        val capturedBuffer = slot<ByteBuffer>()

        every { context.mockSocketChannel.write(capture(capturedBuffer)) } answers {
            clientMessage.bytes.position(clientMessage.bytes.limit())
            clientMessage.bytes.capacity()
        }

        context.writer.sendResponses()
        //then

        context.messageQueue.size.shouldBeZero()
        context.clientBuffers shouldNotContainKey context.key

        capturedBuffer.captured.position(0)
            .remainingAsString(charset = Charsets.US_ASCII) shouldBeEqual expectedMessage
    }

    @Test
    fun `Finishes sending message when not all bytes are written in one go`() {
        val context = initContext()
        context.registry[context.key] = Client(
            context.key, mockk(), mockk(),
            context.mockSocketChannel
        )

        val expectedMessage = "Hello world!";

        val expectedSecondPart = "world!";
        val firstBytesBatch = 6

        val clientMessage = createMessage(expectedMessage, context.key)
        context.messageQueue.offer(clientMessage)

        every { context.mockSocketChannel.write(any<ByteBuffer>()) } answers {
            clientMessage.bytes.position(firstBytesBatch)
            firstBytesBatch
        }

        context.writer.sendResponses()
        context.clientBuffers shouldContainKey context.key
        context.clientBuffers[context.key]!!.queue.let { queue ->
            queue.shouldHaveSize(1)
            queue.peek().remaining().shouldBeExactly(6)

            queue.peek().slice(6, queue.peek().remaining())
                .remainingAsString(Charsets.US_ASCII) shouldBeEqual expectedSecondPart
        }
        context.clientBuffers[context.key]!!.droppedMessages.shouldBeZero()


        every { context.mockSocketChannel.write(any<ByteBuffer>()) } answers {
            clientMessage.bytes.position(clientMessage.bytes.limit())
            clientMessage.bytes.remaining()
        }

        context.writer.sendResponses()
        context.clientBuffers shouldNotContainKey context.key
    }

    @Test
    fun `Finishes sending messages when system buffer is overflown`() {
        val context = initContext()
        context.registry[context.key] = Client(
            context.key, mockk(), mockk(),
            context.mockSocketChannel
        )

        val expectedMessage = "Hello world!";

        val expectedSecondPart = "world!";
        val firstBytesBatch = 6

        val clientMessage = createMessage(expectedMessage, context.key)
        context.messageQueue.offer(clientMessage)

        every { context.mockSocketChannel.write(any<ByteBuffer>()) } answers {
            clientMessage.bytes.position(firstBytesBatch)
            0
        }

        context.writer.sendResponses()
        context.clientBuffers shouldContainKey context.key
        context.clientBuffers[context.key]!!.queue.let { queue ->
            queue.shouldHaveSize(1)
            queue.peek().remaining().shouldBeExactly(6)

            queue.peek().slice(6, queue.peek().remaining())
                .remainingAsString(Charsets.US_ASCII) shouldBeEqual expectedSecondPart
        }
        context.clientBuffers[context.key]!!.droppedMessages.shouldBeZero()


        every { context.mockSocketChannel.write(any<ByteBuffer>()) } answers {
            clientMessage.bytes.position(clientMessage.bytes.limit())
            clientMessage.bytes.remaining()
        }

        context.writer.sendResponses()
        context.clientBuffers shouldNotContainKey context.key
    }

    @Test
    fun `Processes a backlog of messages to the client`() {
        val context = initContext()
        context.registry[context.key] = Client(
            context.key, mockk(), mockk(), context.mockSocketChannel
        )

        context.clientBuffers[context.key] = ClientBuffer(
            ArrayDeque()
        )

        val messages = listOf(
            createMessage("Hello world", context.key),
            createMessage("Hey there", context.key)
        )

        messages.forEach {
            context.clientBuffers[context.key]?.let { buffer ->
                buffer.queue.offer(it.bytes)
                buffer.totalSize += it.bytes.remaining()
            }
        }

        every { context.mockSocketChannel.write(any<ByteBuffer>()) } answers {
            firstArg<ByteBuffer>().position(firstArg<ByteBuffer>().limit())

            val bytes = firstArg<ByteBuffer>().remaining()

            bytes
        }

        context.writer.sendResponses()
        context.writer.sendResponses()

        verify(exactly = 2) { context.mockSocketChannel.write(any<ByteBuffer>()) }

        context.clientBuffers shouldNotContainKey context.key
    }

    private fun initContext(): TestContext {
        val messageQueue = ConcurrentLinkedQueue<ClientMessage>()
        val registry = inMemoryClientRegistry<String, String>()
        val clientBuffers = mutableMapOf<ClientKey, ClientBuffer>()
        val key = ClientKey.fromRemoteAddress("localhost")
        val socketChannel = mockk<SocketChannel>()

        return TestContext(
            messageQueue, registry, key, clientBuffers,
            socketChannel, ResponseWriter(
                clientBuffers, messageQueue, registry
            )
        )
    }

    private fun createMessage(
        content: String,
        clientKey: ClientKey = ClientKey.fromRemoteAddress("localhost")
    ) = ClientMessage(
        ByteBuffer.wrap(content.toByteArray(charset = Charsets.US_ASCII)),
        clientKey
    )
}
