package json

import io.github.akmal2409.nats.server.json.FlatJsonMarshaller
import io.github.akmal2409.nats.server.json.JsonValue
import io.github.akmal2409.nats.server.json.Lexer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
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


    @Test
    fun `Throws IllegalArgumentException when no object passed or malformed`() {
        val invalidInputs = listOf("", "{", "}", "{}}", "{{}")

        assertIllegalArgumentException(invalidInputs)
    }

    @Test
    fun `Throws IllegalArgumentException when key or value are malformed`() {
        val invalidInputs = listOf("{someKey: 123}", "{\"key\": value}",
            "{\"key\": {}}")

        assertIllegalArgumentException(invalidInputs)
    }

    private fun assertIllegalArgumentException(inputs: List<String>) {
        val marshaller = FlatJsonMarshaller(Lexer())

        inputs.forEach { input ->
            withClue("Expected input $input to throw illegal argument exception") {
                shouldThrow<IllegalArgumentException> {
                    marshaller.unmarshall(input)
                }
            }
        }
    }
}
