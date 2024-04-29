package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.server.json.FlatJsonMarshaller
import io.github.akmal2409.knats.server.json.JsonMarshaller
import io.github.akmal2409.knats.server.json.JsonValue
import io.github.akmal2409.knats.server.parser.ConnectOperation
import io.github.akmal2409.knats.server.parser.ParsingError
import io.github.akmal2409.knats.server.parser.ParsingResult
import io.github.akmal2409.knats.server.parser.PendingParsing
import io.github.akmal2409.knats.server.parser.PongOperation
import io.github.akmal2409.knats.server.parser.SubscribeOperation
import io.github.akmal2409.knats.server.parser.SuspendableParser
import io.github.akmal2409.knats.transport.DeserializationResult
import io.github.akmal2409.knats.transport.RequestDeserializer
import io.github.akmal2409.knats.transport.common.nextAsciiToken
import java.nio.ByteBuffer

class RequestDeserializer(
    private val parser: SuspendableParser,
    private val jsonMarshaller: FlatJsonMarshaller
) : RequestDeserializer<Request> {


    override fun deserialize(bytes: ByteBuffer): DeserializationResult<Request> {
        return when (val result = parser.tryParse(bytes)) {
            is PendingParsing -> DeserializationResult.of(null)
            is ParsingError -> error("Parsing error $result")
            else -> DeserializationResult.of(convertToRequest(result, jsonMarshaller))
        }
    }

    override fun close() {
        parser.close()
    }
}

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
                return SubscribeRequest(
                    subject = subject,
                    subscriptionId = id
                )
            }

            return SubscribeRequest(subject, queueGroupOrId, id)
        }
    }
}

data object PongRequest : Request

data class ConnectRequest(
    val verbose: Boolean = true
) : Request


data object PingResponse : Response {
    fun toByteBuffer(): ByteBuffer =
        ByteBuffer.wrap("PING\r\n".toByteArray(charset = Charsets.US_ASCII))
}

data object OkResponse : Response {
    fun toByteBuffer(): ByteBuffer =
        ByteBuffer.wrap("+OK\r\n".toByteArray(charset = Charsets.US_ASCII))
}

data class ErrorResponse internal constructor(val message: String) : Response {

    companion object {

        fun authenticationTimeout() = ErrorResponse("Authentication Timeout")
    }

    fun toByteBuffer(): ByteBuffer =
        ByteBuffer.wrap("-ERR '$message'\r\n".toByteArray(charset = Charsets.US_ASCII))
}

data class MessageResponse(
    val subject: String,
    val subscriptionId: String,
    val payloadSize: Int,
    val payload: ByteBuffer,
    val replyTo: String? = null
) : Response


fun convertToRequest(parsingResult: ParsingResult, jsonMarshaller: JsonMarshaller) =
    when (parsingResult) {
        is ConnectOperation -> convertToConnectRequest(parsingResult, jsonMarshaller)
        is PongOperation -> PongRequest
        is SubscribeOperation -> SubscribeRequest.fromTokens(
            parsingResult.argsBuffer.nextAsciiToken(),
            parsingResult.argsBuffer.nextAsciiToken(), parsingResult.argsBuffer.nextAsciiToken()
        )

        else -> error("UNSUPPORTED OP")
    }

fun convertToConnectRequest(
    connectOperation: ConnectOperation,
    jsonMarshaller: JsonMarshaller
): ConnectRequest {
    val json = connectOperation.toString()
    connectOperation.argsBuffer.clear()

    val parsed = jsonMarshaller.unmarshall(json)

    return ConnectRequest(
        verbose = parsed["verbose"]?.apply {
            require(type == JsonValue.JsonType.BOOLEAN) { "Verbose must be boolean arg" }
        }?.asBoolean() ?: true
    )
}

fun convertToResponse(response: Response): ByteBuffer = when (response) {
    is PingResponse -> response.toByteBuffer()
    is OkResponse -> response.toByteBuffer()
    is ErrorResponse -> response.toByteBuffer()
    else -> error("Unsupported response type ${response::class.qualifiedName}")
}

