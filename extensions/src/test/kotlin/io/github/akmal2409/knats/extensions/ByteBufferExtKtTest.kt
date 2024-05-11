package io.github.akmal2409.knats.extensions

import io.kotest.assertions.withClue
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer
import kotlin.test.Test


class ByteBufferExtKtTest {

    @Test
    fun `toAsciiChar converts all characters`() {

        for (expectedCh in 0 until 256) {
            val byte = expectedCh.toByte()

            val mappedCh = byte.toAsciiChar()

            withClue("${Char(expectedCh)} is not like mapped $mappedCh") {
                expectedCh shouldBe  mappedCh.code
            }
        }
    }

    @Test
    fun `nextAsciiToken returns null when buffer is empty`() {
        assertNextAsciiToken("", null)
    }

    @Test
    fun `nextAsciiToken returns tokens when present`() {
        assertNextAsciiToken("token1 token2", "token1", "token2", null)
    }

    private fun assertNextAsciiToken(input: String, vararg expectedTokens: String?) {
        val bytes = ByteBuffer.wrap(input.toByteArray(Charsets.US_ASCII))

        expectedTokens.forEach { token ->
            bytes.nextAsciiToken() shouldBe token
        }
    }

    @Test
    fun `remainingAsString parses string without whitespace`() {
        assertRemainingAsStringFor("some_text")
    }

    @Test
    fun `remainingAsString parses with whitespace whole sentence`() {
        assertRemainingAsStringFor("Some long long sentence with another line \n Yes another line here")

    }

    private fun assertRemainingAsStringFor(input: String) {
        val bytes = ByteBuffer.wrap(input.toByteArray(Charsets.US_ASCII))

        bytes.remainingAsString(Charsets.US_ASCII) shouldBe input
        bytes.hasRemaining().shouldBeFalse()
    }



}
