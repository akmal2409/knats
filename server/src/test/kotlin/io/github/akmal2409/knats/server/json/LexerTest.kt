package io.github.akmal2409.knats.server.json

import io.kotest.matchers.collections.shouldContainExactly
import kotlin.test.Test

class LexerTest {


    @Test
    fun `Lexes json string`() {
        val input =
            "{\"num\": 123, \"bool\": true, \"float\": 2.004, \"array\": [\"Test1\",\"test2\"], \"str\": \"some\"}"

        val tokens = Lexer().lex(input)

        val expectedTokens = listOf(
            '{', "num", ':', 123, ',', "bool", ':', true, ',', "float", ':', 2.004f,
            ',', "array", ':', '[', "Test1", ',', "test2", ']', ',', "str", ':', "some", '}'
        )

        tokens.unbox() shouldContainExactly expectedTokens
    }

    private fun List<SymbolOrValue>.unbox() = map { container ->
        if (container.value != null) {
            container.value.value
        } else {
            container.symbol!!.char
        }
    }
}
