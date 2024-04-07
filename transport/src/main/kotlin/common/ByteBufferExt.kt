package io.github.akmal2409.nats.transport.common

import java.nio.ByteBuffer
import java.nio.charset.Charset

fun ByteBuffer.remainingAsString(charset: Charset = Charsets.UTF_8) =
    charset.decode(this).toString()

fun ByteBuffer.nextAsciiToken(): String {
    if (!hasRemaining()) return ""

    val sb = StringBuilder()

    var byte: Byte = get()

    while (hasRemaining() && !byte.toAsciiChar().isWhitespace()) {
        sb.append(byte.toAsciiChar())
        byte = get()
    }

    return sb.toString()
}

fun Byte.toAsciiChar() = (this.toInt() and 0xff).toChar()
