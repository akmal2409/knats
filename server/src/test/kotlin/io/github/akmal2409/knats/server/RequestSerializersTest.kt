package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.server.json.JsonValue
import io.github.akmal2409.knats.server.mocks.JsonMarshallerMock
import io.github.akmal2409.knats.server.parser.ConnectOperation
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import java.lang.IllegalArgumentException
import java.nio.ByteBuffer
import kotlin.test.Test


class RequestSerializersTest {

    @Test
    fun `Convert ConnectCommand parses arguments`() {
        val marshaller =
            JsonMarshallerMock(mapOf("verbose" to JsonValue(JsonValue.JsonType.BOOLEAN, false)))
        val argsJson = "{\"verbose\": false}"

        val connect = convertToConnectRequest(
            ConnectOperation(ByteBuffer.wrap(argsJson.toByteArray(Charsets.US_ASCII))),
            marshaller
        )

        marshaller.inputJsons[0] shouldBe argsJson
        connect.verbose.shouldBeFalse()
    }

    @Test
    fun `Throws IllegalArgumentException if verbose is non-boolean`() {
        val marshaller =
            JsonMarshallerMock(mapOf("verbose" to JsonValue(JsonValue.JsonType.STRING, "true")))
        val argsJson = "{\"verbose\": \"true\"}"

        shouldThrow<IllegalArgumentException> {
            convertToConnectRequest(
                ConnectOperation(ByteBuffer.wrap(argsJson.toByteArray(Charsets.US_ASCII))),
                marshaller
            )
        }
    }

    @Test
    fun `Applies default if verbose is missing`() {
        val marshaller = JsonMarshallerMock(mapOf())
        val argsJson = "{}"

        val connect = convertToConnectRequest(
            ConnectOperation(ByteBuffer.wrap(argsJson.toByteArray(Charsets.US_ASCII))),
            marshaller
        )

        connect.verbose.shouldBeTrue()
    }
}
