package parser;

import io.github.akmal2409.nats.server.common.remainingAsString
import java.nio.ByteBuffer;

sealed interface ParsingResult

data class ParsingError(val reason: String, val throwable: Throwable? = null) : ParsingResult {
    companion object {
        val UNKNOWN_COMMAND = ParsingError("Unknown command supplied")
        val MISSING_ARGUMENTS = ParsingError("Arguments are missing")
        val INVALID_PAYLOAD_SIZE =
            ParsingError("Invalid payload size. Either negative or exceeds max config")
        val INVALID_STRING_TERMINATION = ParsingError("New line character expected")
        val MAX_ARG_SIZE_EXCEEDED = ParsingError("Max arg size exceeded")
    }
}

class ConnectCommand(val argsBuffer: ByteBuffer) : ParsingResult {

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

class PongOperation() : ParsingResult
