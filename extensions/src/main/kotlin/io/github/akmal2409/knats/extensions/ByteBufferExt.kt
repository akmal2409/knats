package io.github.akmal2409.knats.extensions

import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * Reads everything from current position till the end in desired encoding.
 * Important: Moves position of the buffer's cursor forward
 */
fun ByteBuffer.remainingAsString(charset: Charset = Charsets.UTF_8) =
    charset.decode(this).toString()

/**
 * Retrieves next ASCII token that is before whitespace
 */
fun ByteBuffer.nextAsciiToken(): String? {
    if (!hasRemaining()) return null

    return buildString {

        var ch = get().toAsciiChar()

        while (ch.isWhitespace() && hasRemaining()) {
            ch = get().toAsciiChar()
        }

        if (!ch.isWhitespace()) {
            append(ch)

            while (hasRemaining() && !ch.isWhitespace()) {
                ch = get().toAsciiChar()
                if (ch.isWhitespace()) {
                    position(position() - 1)
                    break
                }

                append(ch)
            }

        }

    }.takeIf { it.isNotEmpty() }
}

fun Byte.toAsciiChar() = (this.toInt() and 0xff).toChar()

fun Char.toAsciiByte() = (this.code.toByte())

fun ByteBuffer.putAsciiString(text: String) {

    for (ch in text) {
        put(ch.toAsciiByte())
    }
}
