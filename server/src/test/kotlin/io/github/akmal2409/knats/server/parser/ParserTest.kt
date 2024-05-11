package io.github.akmal2409.knats.server.parser

import io.github.akmal2409.knats.extensions.remainingAsString
import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ParserTest {

    val defaultConnectOptions = ConnectOptions(
        verbose = true,
        pedantic = true, tlsRequired = false
    )

    val defaultParser = SuspendableParser(200, 100)

    @Test
    fun `Parses CONNECT string with arguments`() {
        val expectedArgs = defaultConnectOptions.toJson()
        val bytes = commandAsBytes("CONNECT $expectedArgs\r\n")
        val parsingResult = defaultParser.tryParse(bytes)

        assertIs<ConnectOperation>(parsingResult)

        val args = parsingResult.argsBuffer.remainingAsString(Charsets.US_ASCII)

        assertEquals(expectedArgs, args)
    }

    @Test
    fun `Parses Connect string without arguments`() {
        val bytes = commandAsBytes("CONNECT {}\r\n")
        val parsingResult = defaultParser.tryParse(bytes)

        assertIs<ConnectOperation>(parsingResult)
        val args = parsingResult.argsBuffer.remainingAsString(Charsets.US_ASCII)

        assertEquals("{}", args)
    }

    @Test
    fun `Returns error when CONNECT without args`() {
        val bytes = commandAsBytes("CONNECT \r\n")
        val parsingResult = defaultParser.tryParse(bytes)

        assertEquals(ParsingError.INVALID_CLIENT_PROTOCOL, parsingResult)
    }

    @Test
    fun `Returns error when invalid message beginning`() {
        val bytes = commandAsBytes("SOMETHING_ELSE")
        val parsingResult = defaultParser.tryParse(bytes)

        assertEquals(ParsingError.INVALID_CLIENT_PROTOCOL, parsingResult)
    }

    @Test
    fun `Parses SUB command with arguments`() {
        val args = "test 123"
        val bytes = commandAsBytes("SUB $args\r\n")
        val result = defaultParser.tryParse(bytes)

        assertIs<SubscribeOperation>(result)
        assertEquals(args, result.argsBuffer.remainingAsString())
    }

    @Test
    fun `Parses PUB command with args and payload`() {
        val args = "sub1 4"
        val bytes = commandAsBytes("PUB $args\r\ntest\r\n")
        val result = defaultParser.tryParse(bytes)

        assertIs<PublishOperation>(result)
        assertEquals(args, result.argsBuffer.remainingAsString())
        assertEquals("test", result.payloadBuffer.remainingAsString())
    }


    @Test
    fun `Fails to parse when less bytes in payload than declared`() {
        val args = "sub1 5"
        val bytes = commandAsBytes("PUB $args\r\ntest\r\n")
        val result = defaultParser.tryParse(bytes)

        assertEquals(ParsingError.MAXIMUM_PAYLOAD_VIOLATION, result)
    }

    @Test
    fun `Fails to parse when more bytes in payload than declared`() {
        val args = "sub1 3"
        val bytes = commandAsBytes("PUB $args\r\ntest\r\n")
        val result = defaultParser.tryParse(bytes)

        assertEquals(ParsingError.MAXIMUM_PAYLOAD_VIOLATION, result)
    }

    @Test
    fun `Parses payload with byte count 0`() {
        val args = "sub1 0"
        val bytes = commandAsBytes("PUB $args\r\n\r\n")
        val result = defaultParser.tryParse(bytes)

        assertIs<PublishOperation>(result)
        assertEquals(0, result.payloadBuffer.limit())
    }

    @Test
    fun `Parses payload with CRLF inside`() {
        val bytes = commandAsBytes("PUB sub1 2\r\n\r\n\r\n")
        val result = defaultParser.tryParse(bytes)

        assertIs<PublishOperation>(result)
        assertEquals("\r\n", result.payloadBuffer.remainingAsString())
    }

    @Test
    fun `Returns error if arguments are missing for SUB`() {
        val bytes = commandAsBytes("SUB \r\n")
        val result = defaultParser.tryParse(bytes)

        assertEquals(ParsingError.INVALID_CLIENT_PROTOCOL, result)
    }

    @Test
    fun `Incomplete command returns pending parsing state`() {
        val bytes = commandAsBytes("CONNE")
        val parsingResult = defaultParser.tryParse(bytes)

        assertIs<PendingParsing>(parsingResult)
        assertEquals(bytes.limit(), parsingResult.bytesRead)
    }


    @Test
    fun `Parses PONG command`() {
        val bytes = commandAsBytes("PONG\r\n")
        val parsingResult = defaultParser.tryParse(bytes)

        assertIs<PongOperation>(parsingResult)
    }

    private fun commandAsBytes(command: String) =
        ByteBuffer.wrap(command.toByteArray(charset = Charsets.US_ASCII))
}
