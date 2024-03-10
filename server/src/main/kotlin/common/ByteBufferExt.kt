package io.github.akmal2409.nats.server.common

import java.nio.ByteBuffer
import java.nio.charset.Charset

fun ByteBuffer.remainingAsString(charset: Charset = Charsets.UTF_8) =
    charset.decode(this).toString()
