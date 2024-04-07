package io.github.akmal2409.nats.server


import io.github.akmal2409.nats.server.parser.ConnectOperation
import io.github.akmal2409.nats.server.parser.ParsingResult
import io.github.akmal2409.nats.server.parser.PongOperation
import io.github.akmal2409.nats.server.parser.SubscribeOperation
import io.github.akmal2409.nats.transport.common.nextAsciiToken
import java.nio.ByteBuffer


sealed interface Request

sealed interface Response

data class PublishRequest(
    val subject: String,
    val payloadSize: Int,
    val payload: ByteBuffer,
    val replyTo: String? = null
) : Request

data class SubscribeRequest(
    val subject: String,
    val subscriptionId: String,
    val queueGroup: String? = null
) : Request {
    companion object {

        fun fromTokens(subject: String, queueGroupOrId: String, id: String): SubscribeRequest {
            if (id.isEmpty()) {
                return SubscribeRequest(subject = subject,
                    subscriptionId = id)
            }

            return SubscribeRequest(subject, queueGroupOrId, id)
        }
    }
}

data object PongRequest : Request

data class ConnectRequest(
    val verbose: Boolean = true
) : Request

data object PingResponse : Response

data class MessageResponse(
    val subject: String,
    val subscriptionId: String,
    val payloadSize: Int,
    val payload: ByteBuffer,
    val replyTo: String? = null
) : Response


fun convertToRequest(parsingResult: ParsingResult) = when(parsingResult) {
    is ConnectOperation -> ConnectRequest()
    is PongOperation -> PongRequest
    is SubscribeOperation -> SubscribeRequest.fromTokens(parsingResult.argsBuffer.nextAsciiToken(),
        parsingResult.argsBuffer.nextAsciiToken(), parsingResult.argsBuffer.nextAsciiToken())
    else -> error("UNSUPPORTED OP")
}

fun convertToResponse(response: Response): ByteBuffer = when(response) {
    is PingResponse -> ByteBuffer.wrap("PING\r\n".toByteArray(charset = Charsets.US_ASCII))
    else -> error("LOL")
}

