package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.ErrorResponse
import io.github.akmal2409.knats.server.OkResponse
import io.github.akmal2409.knats.server.PingResponse
import io.github.akmal2409.knats.server.Response
import io.github.akmal2409.knats.extensions.remainingAsString
import java.nio.ByteBuffer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector


fun ByteBuffer.convertResponse(): Response {
    val text = remainingAsString(Charsets.US_ASCII).stripLineEnding()

    return when {
        text.startsWith("-ERR ") -> ErrorResponse(text.removePrefix("-ERR ")
            .replace("'", ""))

        text == "+OK" -> OkResponse
        text == "PING" -> PingResponse
        else -> error("Unknown response type received $text")
    }
}


fun ByteBuffer.toErrResponse(): ErrorResponse {
    val text = remainingAsString(Charsets.US_ASCII)

    if (!text.startsWith("-ERR ")) {
        error("Cannot parse as error response $text")
    }

    return ErrorResponse(text.removePrefix("-ERR ")
        .replace("'", "")
        .stripLineEnding())
}

fun String.stripLineEnding() = this.removeSuffix("\r\n")

suspend fun <T> Flow<T>.collectWithException(collector: FlowCollector<T>): Throwable? = try {
    collect(collector)
    null
} catch (ex: Throwable) {
    ex
}
