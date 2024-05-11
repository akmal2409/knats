package io.github.akmal2409.knats.server.parser;

import io.github.akmal2409.knats.extensions.remainingAsString
import java.nio.ByteBuffer;

sealed interface ParsingResult

data class ParsingError(val reason: String, val throwable: Throwable? = null) : ParsingResult {
    companion object {
        val INVALID_CLIENT_PROTOCOL = ParsingError("Invalid Client Protocol")
        val MAXIMUM_PAYLOAD_VIOLATION =
            ParsingError("Maximum Payload Violation")
        val INVALID_STRING_TERMINATION = ParsingError("New line character expected")
    }
}

class ConnectOperation(val argsBuffer: ByteBuffer) : ParsingResult {

    override fun toString(): String = argsBuffer.remainingAsString()
}

class SubscribeOperation(val argsBuffer: ByteBuffer) : ParsingResult {

    override fun toString(): String = argsBuffer.remainingAsString()
}

class PublishOperation(val argsBuffer: ByteBuffer, val payloadBuffer: ByteBuffer) : ParsingResult {
    override fun toString(): String =
        "args=[${argsBuffer.remainingAsString()}] payload=[${payloadBuffer.remainingAsString()}]"
}

class PendingParsing(val bytesRead: Int) : ParsingResult {
    override fun toString(): String = "bytesRead=$bytesRead"
}

class PongOperation : ParsingResult
