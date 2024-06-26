package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.extensions.nextAsciiToken
import io.github.akmal2409.knats.extensions.putAsciiString
import io.github.akmal2409.knats.extensions.toAsciiByte
import io.github.akmal2409.knats.server.common.numberOfDigits
import io.github.akmal2409.knats.server.json.FlatJsonMarshaller
import io.github.akmal2409.knats.server.json.JsonMarshaller
import io.github.akmal2409.knats.server.json.JsonValue
import io.github.akmal2409.knats.server.parser.ConnectOperation
import io.github.akmal2409.knats.server.parser.ParsingError
import io.github.akmal2409.knats.server.parser.ParsingResult
import io.github.akmal2409.knats.server.parser.PendingParsing
import io.github.akmal2409.knats.server.parser.PingOperation
import io.github.akmal2409.knats.server.parser.PongOperation
import io.github.akmal2409.knats.server.parser.PublishOperation
import io.github.akmal2409.knats.server.parser.SubscribeOperation
import io.github.akmal2409.knats.server.parser.SuspendableParser
import io.github.akmal2409.knats.transport.DeserializationResult
import io.github.akmal2409.knats.transport.RequestDeserializer
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

sealed interface Response {

    fun toByteBuffer(): ByteBuffer
}

data class PublishRequest(
    val subject: String,
    val payloadSize: Int,
    val payload: ByteBuffer,
    val replyTo: String? = null
) : Request {

    companion object {

        @JvmStatic
        fun fromOperation(operation: PublishOperation): PublishRequest {
            val subject: String? = operation.argsBuffer.nextAsciiToken()
            val replyTo: String? = operation.argsBuffer.nextAsciiToken() // optional
            val byteCountStr: String? = operation.argsBuffer.nextAsciiToken()

            requireNotNull(subject) {
                "Subject was not provided for PUB"
            }
            val bytes: Int

            return if (byteCountStr == null) {
                requireNotNull(replyTo) { "Byte size is missing for PUB" }
                // replyTo becomes byte count
                bytes = replyTo.toIntOrNull() ?: error("Byte size is not an integer")
                PublishRequest(
                    subject, bytes, operation.payloadBuffer
                )
            } else {
                requireNotNull(replyTo) { "ReplyTo is missing for PUB" }
                bytes = byteCountStr.toIntOrNull() ?: error("Byte size is not an integer")

                PublishRequest(subject, bytes, operation.payloadBuffer, replyTo)
            }
        }
    }
}

data class InfoResponse(
    val serverId: String,
    val serverName: String,
    val version: String,
    val go: String,
    val host: String,
    val port: Int,
    val maxPayload: Int,
    val proto: Int,
    val headers: Boolean = false
) : Response {

    override fun toString(): String = "INFO " + """{"server_id": "$serverId",
        "server_name": "$serverName",
        "version": "$version",
        "go": "$go",
        "host": "$host",
        "port": $port,
        "max_payload": $maxPayload,
        "proto": $proto,
        "headers": $headers}""".replace(Regex("\\s"), "") + "\r\n"
    override fun toByteBuffer(): ByteBuffer = ByteBuffer.wrap(toString().toByteArray(Charsets.US_ASCII))
}

data class SubscribeRequest(
    val subject: String,
    val subscriptionId: String,
    val queueGroup: String? = null
) : Request {
    companion object {

        @JvmStatic
        fun fromArgsBuffer(buf: ByteBuffer): SubscribeRequest {
            val subject: String? = buf.nextAsciiToken()
            val queueGroupOrId: String? = buf.nextAsciiToken()
            val id: String? = buf.nextAsciiToken()

            requireNotNull(subject) { "Subject is not present" }
            requireNotNull(queueGroupOrId) { "Queue group or ID not encountered" }

            return if (id == null) {
                SubscribeRequest(subject, queueGroupOrId)
            } else {
                SubscribeRequest(subject, id, queueGroupOrId)
            }
        }
    }
}


data object PongRequest : Request

data object PingRequest : Request

data class ConnectRequest(
    val verbose: Boolean = true
) : Request


data object PingResponse : Response {
    override fun toByteBuffer(): ByteBuffer =
        ByteBuffer.wrap("PING\r\n".toByteArray(charset = Charsets.US_ASCII))
}

data object PongResponse : Response {
    override fun toByteBuffer(): ByteBuffer =
        ByteBuffer.wrap("PONG\r\n".toByteArray(charset = Charsets.US_ASCII))
}

data object OkResponse : Response {
    override fun toByteBuffer(): ByteBuffer =
        ByteBuffer.wrap("+OK\r\n".toByteArray(charset = Charsets.US_ASCII))
}

data class ErrorResponse internal constructor(val message: String) : Response {

    companion object {

        fun authenticationTimeout() = ErrorResponse("Authentication Timeout")

        fun staleConnection() = ErrorResponse("Stale Connection")

        fun unknownProtocolError() = ErrorResponse("Unknown Protocol Operation")
    }

    override fun toByteBuffer(): ByteBuffer =
        ByteBuffer.wrap("-ERR '$message'\r\n".toByteArray(charset = Charsets.US_ASCII))
}

data class MessageResponse(
    val subject: String,
    val subscriptionId: String,
    val payloadSize: Int,
    val payload: ByteBuffer,
    val replyTo: String? = null
) : Response {

    companion object {

        private const val MSG = "MSG"

        fun from(publishRequest: PublishRequest, subscriptionId: String): MessageResponse =
            MessageResponse(
                subject = publishRequest.subject,
                subscriptionId = subscriptionId,
                payloadSize = publishRequest.payloadSize,
                payload = publishRequest.payload.slice().asReadOnlyBuffer(),
                replyTo = publishRequest.replyTo
            )
    }

    override fun toByteBuffer(): ByteBuffer {
        var totalSize = payloadSize + 4 + payloadSize.numberOfDigits() +
                subscriptionId.length + subject.length + 3 + MSG.length

        if (replyTo != null) totalSize += 1 + replyTo.length

        val buffer = ByteBuffer.allocate(totalSize)
        val space = ' '.toAsciiByte()

        buffer.putAsciiString(MSG)
        buffer.put(space)

        buffer.putAsciiString(subject)
        buffer.put(space)

        buffer.putAsciiString(subscriptionId)
        buffer.put(space)

        replyTo?.let {
            buffer.putAsciiString(it)
            buffer.put(space)
        }

        buffer.putAsciiString(payloadSize.toString())
        buffer.put('\r'.toAsciiByte())
        buffer.put('\n'.toAsciiByte())

        if (payloadSize > 0) buffer.put(payload)
        buffer.put('\r'.toAsciiByte())
        buffer.put('\n'.toAsciiByte())

        buffer.flip()
        return buffer
    }
}


fun convertToRequest(parsingResult: ParsingResult, jsonMarshaller: JsonMarshaller) =
    when (parsingResult) {
        is ConnectOperation -> convertToConnectRequest(parsingResult, jsonMarshaller)
        is PongOperation -> PongRequest
        is SubscribeOperation -> SubscribeRequest.fromArgsBuffer(parsingResult.argsBuffer)
        is PublishOperation -> PublishRequest.fromOperation(parsingResult)
        is PingOperation -> PingRequest
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
