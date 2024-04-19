package io.github.akmal2409.nats.server.json

data class JsonValue(
    val type: JsonType,
    val value: Any
) {
    object NullValue {}

    companion object {
        private fun incompatibleTypeMessageCreator(expected: JsonType, actual: JsonType) =
            "JSON value is of type $actual but requested as $expected"

        fun ofNumber(num: Number) = when (num) {
            is Float -> JsonValue(JsonType.FLOAT, num)
            is Int -> JsonValue(JsonType.INT, num)
            else -> error("Cannot construct json value of a num $num")
        }
    }

    enum class JsonType {
        INT, FLOAT, STRING, ARRAY, BOOLEAN, NULL;

        companion object {

            inline fun <reified T> forType() = when (T::class) {
                String::class -> STRING
                Int::class -> INT
                Float::class -> FLOAT
                Array::class -> ARRAY
                Boolean::class -> BOOLEAN
                else -> error("Unsupported type ${T::class.simpleName}")
            }
        }
    }

    fun asString(): String = getCastValue()

    fun asBoolean(): Boolean = getCastValue()

    fun asArray(): Array<*> = getCastValue()

    fun asInt(): Int = getCastValue()

    fun asFloat(): Int = getCastValue()

    fun <T> asNull(): T? = null

    private inline fun <reified T> getCastValue(): T {
        val actualType = JsonType.forType<T>()

        check(actualType == type) { incompatibleTypeMessageCreator(type, actualType) }
        return value as T
    }
}

enum class JsonSymbol(
    val char: Char
) {

    LEFT_BRACKET('['), RIGHT_BRACKET(']'), LEFT_BRACE('{'), RIGHT_BRACE('}'),
    COMMA(','), COLON(':');

    companion object {

        fun fromSymbol(ch: Char) = entries.find { it.char == ch }
    }
}

class SymbolOrValue(val value: JsonValue? = null, val symbol: JsonSymbol? = null) {

}

class Lexer {

    companion object {
        private val JSON_SYMBOLS = JsonSymbol.entries.map { it.char }.toHashSet()
        private val NUMBER_SYMBOLS = ('0'..'9').toHashSet() + setOf('.', '-', 'e')
        private const val DOUBLE_QUOTE = '"';
        private const val NULL = "null"
        private const val TRUE = "true"
        private const val FALSE = "false"
    }


    fun lex(jsonStr: String): List<SymbolOrValue> {
        val tokens = mutableListOf<SymbolOrValue>()
        var i = 0

        while (i < jsonStr.length) {
            if (jsonStr[i].isWhitespace()) {
                i++
                continue
            }
            val (str, nextIdx) = tryLexString(jsonStr, i)
            i = nextIdx

            if (str != null) {
                tokens.add(SymbolOrValue(value = JsonValue(JsonValue.JsonType.STRING, str)))
                continue
            }

            val (boolean, nextIdx2) = tryLexBoolean(jsonStr, i)
            i = nextIdx2

            if (boolean != null) {
                tokens.add(SymbolOrValue(value = JsonValue(JsonValue.JsonType.BOOLEAN, boolean)))
                continue
            }

            val (number, nextIdx3) = tryLexNumber(jsonStr, i)
            i = nextIdx3

            if (number != null) {
                tokens.add(SymbolOrValue(value = JsonValue.ofNumber(number)))
                continue
            }

            val (isNull, nextIdx4) = tryLexNull(jsonStr, i)
            i = nextIdx4

            if (isNull) {
                tokens.add(
                    SymbolOrValue(
                        value = JsonValue(
                            JsonValue.JsonType.NULL,
                            JsonValue.NullValue
                        )
                    )
                )
                continue
            }

            if (jsonStr[i] in JSON_SYMBOLS) {
                tokens.add(SymbolOrValue(symbol = JsonSymbol.fromSymbol(jsonStr[i])))
                i++
            } else {
                throw RuntimeException("Cannot parse tokens")
            }
        }

        return tokens.toList()
    }

    private fun tryLexString(json: String, i: Int): Pair<String?, Int> {
        if (json[i] != DOUBLE_QUOTE) return null to i

        val builder = StringBuilder()

        var chIdx = i + 1
        while (chIdx < json.length && json[chIdx] != DOUBLE_QUOTE) {
            builder.append(json[chIdx++])
        }

        if (i == json.length) return null to i

        val token = builder.toString()
        return token to chIdx + 1
    }

    private fun tryLexNull(json: String, i: Int): Pair<Boolean, Int> =
        if (json.length - i < NULL.length || json.substring(i, i + NULL.length) != NULL) {
            false to i
        } else {
            true to i + NULL.length
        }

    private fun tryLexBoolean(json: String, i: Int): Pair<Boolean?, Int> {
        val length = json.length - i

        return if (length >= TRUE.length && json.substring(i, i + TRUE.length) == TRUE) {
            true to i + TRUE.length
        } else if (length >= FALSE.length && json.substring(i, i + FALSE.length) == FALSE) {
            false to i + FALSE.length
        } else {
            null to i
        }
    }

    private fun tryLexNumber(json: String, i: Int): Pair<Number?, Int> {
        val builder = StringBuilder()

        var chIdx = i

        while (chIdx < json.length && json[chIdx] in NUMBER_SYMBOLS) {
            builder.append(json[chIdx])
            chIdx++
        }

        if (builder.isEmpty()) return null to i

        return if ('.' in builder) {
            builder.toString().toFloat() to chIdx
        } else {
            builder.toString().toInt() to chIdx
        }
    }
}

class FlatJsonMarshaller(private val lexer: Lexer) {

    fun unmarshall(jsonStr: String): Map<String, JsonValue> {
        val values = mutableMapOf<String, JsonValue>()
        val tokens = lexer.lex(jsonStr)

        if (tokens.isEmpty()) throw RuntimeException("Invalid JSON supplied. Expected at least {} empty object")

        if (tokens[0].symbol != JsonSymbol.LEFT_BRACE || tokens.last().symbol != JsonSymbol.RIGHT_BRACE) {
            throw RuntimeException("Invalid JSON supplied. Malformed braces")
        }

        var i = 1

        while (i < tokens.size - 1) {
            // should be key
            require(tokens[i].value?.type == JsonValue.JsonType.STRING) {
                "Expected key but received ${tokens[i].value}"
            }

            val key = tokens[i].value!!.asString()
            // should be colon
            i++
            require(tokens[i].symbol == JsonSymbol.COLON) {
                "Expected colon after key but received ${tokens[i].value}"
            }

            i++
            // should be value
            val value = tokens[i].value
            require(value != null) { "Expected value after colon" }

            require(key !in values) { "Duplicate key encountered $key" }
            values[key] = value
            i++
            // should be comma or end

            if (i == tokens.size - 2 && tokens[i].symbol == JsonSymbol.COMMA) {
                throw IllegalArgumentException("Comma is present when there is no key")
            }

            if (i < tokens.size - 1 && tokens[i].symbol != JsonSymbol.COMMA) {
                throw IllegalArgumentException("Expected a comma but received ${tokens[i].value}")
            }
            i++
        }

        return values
    }


}

fun main() {
    val marshaller = FlatJsonMarshaller(Lexer())

    println(
        marshaller.unmarshall(
            "{\"num\": 123, \"bool\": true, \"float\": 2.004,  \"str\": \"some\"}"
        ).entries.map{(key, value) -> "$key=$value"}
    )
}
