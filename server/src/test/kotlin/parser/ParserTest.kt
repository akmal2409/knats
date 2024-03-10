package parser

import io.github.akmal2409.nats.server.common.remainingAsString
import io.github.akmal2409.nats.server.parser.*
import java.nio.ByteBuffer
import kotlin.test.*

class ParserTest {

    val defaultOptions = Options(200, 100)
    val defaultConnectOptions = ConnectOptions()

    @Test
    fun `Parses CONNECT string with arguments`() {
        val expectedArgs = defaultConnectOptions.toJson()
        val commandStr = "CONNECT $expectedArgs\r\n"
        val bytes = ByteBuffer.wrap(commandStr.toByteArray(charset = Charsets.US_ASCII))
        val context = Context.initial()

        val operationOrError = parse(bytes, context, defaultOptions)

        assertNull(operationOrError.leftOrNull())
        val operation = operationOrError.getOrNull()
        assertNotNull(operation)

        assertTrue { operation is ConnectOperation }

        val args = (operation as ConnectOperation).argsBuffer!!.remainingAsString(Charsets.US_ASCII)

        assertEquals(expectedArgs, args)
    }

    @Test
    fun `Returns error when invalid message beginning`() {

    }


}
