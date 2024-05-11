package io.github.akmal2409.knats.server.util

import java.nio.ByteBuffer


fun createArgsBuffer(vararg args: String): ByteBuffer {
    val argString = buildString {
        args.forEachIndexed { i, arg ->
            append(arg)

            if (i < args.lastIndex) {
                append(' ')
            }
        }
    }

    return ByteBuffer.wrap(argString.toByteArray(Charsets.US_ASCII))
}
