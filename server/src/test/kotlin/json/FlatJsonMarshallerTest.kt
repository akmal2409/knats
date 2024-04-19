package json

import io.github.akmal2409.nats.server.json.FlatJsonMarshaller
import io.github.akmal2409.nats.server.json.JsonValue
import io.github.akmal2409.nats.server.json.Lexer
import io.kotest.matchers.maps.shouldContainExactly
import org.junit.jupiter.api.Test


class FlatJsonMarshallerTest {

    @Test
    fun `Unmarshalls string with primitives`() {
        val input =
            "{\"num\": 123, \"bool\": true, \"float\": 2.004, \"str\": \"some\"}"

        val expected = mapOf(
            "num" to JsonValue(JsonValue.JsonType.INT, 123),
            "bool" to JsonValue(JsonValue.JsonType.BOOLEAN, true),
            "float" to JsonValue(JsonValue.JsonType.FLOAT, 2.004f),
            "str" to JsonValue(JsonValue.JsonType.STRING, "some")
        )

        val marshaller = FlatJsonMarshaller(Lexer())

        marshaller.unmarshall(input) shouldContainExactly expected
    }
}
