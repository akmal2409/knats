package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.ErrorResponse
import io.github.akmal2409.knats.server.OkResponse
import io.github.akmal2409.knats.server.PingResponse
import io.github.akmal2409.knats.server.Response
import io.github.akmal2409.knats.extensions.remainingAsString
import io.github.akmal2409.knats.server.InfoResponse
import io.github.akmal2409.knats.server.json.FlatJsonMarshaller
import io.github.akmal2409.knats.server.json.JsonValue
import io.github.akmal2409.knats.server.json.Lexer
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
        text.startsWith("INFO") -> text.removePrefix("INFO ").toInfoResponse()
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

fun String.toInfoResponse(): InfoResponse {
    val jsonParser = FlatJsonMarshaller(Lexer())

    val json = jsonParser.unmarshall(this)

    return infoResponseFromJson(json)
}

fun infoResponseFromJson(json: Map<String, JsonValue>): InfoResponse = InfoResponse(
    json["server_id"]!!.asString(),
    json["server_name"]!!.asString(),
    json["version"]!!.asString(),
    json["go"]!!.asString(),
    json["host"]!!.asString(),
    json["port"]!!.asInt(),
    json["max_payload"]!!.asInt(),
    json["proto"]!!.asInt(),
    json["headers"]!!.asBoolean()
)

fun String.stripLineEnding() = this.removeSuffix("\r\n")

suspend fun <T> Flow<T>.collectWithException(collector: FlowCollector<T>): Throwable? = try {
    collect(collector)
    null
} catch (ex: Throwable) {
    ex
}
