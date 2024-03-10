package io.github.akmal2409.nats.server.parser

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import io.github.akmal2409.nats.server.parser.ParsingError.Companion.MAX_ARG_SIZE_EXCEEDED
import io.github.akmal2409.nats.server.parser.ParsingError.Companion.UNKNOWN_COMMAND
import java.nio.ByteBuffer

sealed interface Operation {

}

object UnparsedOperation: Operation

class ConnectOperation(
    val argsBuffer: ByteBuffer
): Operation

/**
 * To implement somewhat zero allocation parsing, we need to have states to define transitions
 */
enum class State {
    OP_START,
    OP_C, OP_CO, OP_CON, OP_CONN, OP_CONNE, OP_CONNEC, OP_CONNECT, OP_CONNECT_ARG, // will follow json like conifg {}

    OP_P, OP_PO, OP_PON, OP_PONG, // no args after

    OP_S, OP_SU, OP_SUB, // with subject, optionally queue group and sub_id after

    OP_PU, OP_PUB, // with subject, optionally reply-to, byte count + payload
}

const val CRLF_BEGIN = '\r'
const val CRLF_END = '\n'

/**
 * Since parsing will be done using non-blocking IO, we need a way to store (checkpoint)
 * our parsers execution in a context
 */
class Context(
    var state: State,
    var whitespaceAfterCommand: Int = 0,
    var carriageReturnEncountered: Boolean = false,
    var argsBuffer: ByteBuffer? = null,
    var headerBuffer: ByteBuffer? = null,
    var payloadBuffer: ByteBuffer? = null
) {

    companion object {

        fun initial(): Context = Context(State.OP_START)
    }

    inline fun moveStateIf(
        targetState: State,
        predicate: () -> Boolean,
        onError: () -> ParsingError
    ): ParsingError? {
        if (predicate()) {
            state = targetState
            return null
        } else {
            return onError()
        }
    }

}

class Options(
    val maxPayloadSize: Int,
    val maxArgSize: Int
)

data class ParsingError(val reason: String) {
    companion object {
        val UNKNOWN_COMMAND = ParsingError("Unknown command supplied")
        val MAX_ARG_SIZE_EXCEEDED = ParsingError("Max arg size exceeded")
    }
}

fun Char.isWhitespace() = this == ' ' || this == '\t'

fun parse(
    bytes: ByteBuffer,
    context: Context,
    options: Options,
    byteBuffFactory: (Int) -> ByteBuffer = { ByteBuffer.allocate(it) }
): Either<ParsingError, Operation> {
    var bytesRead = 0

    while (bytes.hasRemaining()) {
        val b = bytes.get()
        bytesRead++
        val opCh = (b.toInt() and 0xff).toChar()
        var error: ParsingError? = null
        var operation: Operation? = null

        when (context.state) {
            State.OP_START -> {
                error = context.moveStateIf(State.OP_C, { opCh == 'C' || opCh == 'c' }) {
                    UNKNOWN_COMMAND
                }
            }

            State.OP_C -> {
                error = context.moveStateIf(State.OP_CO, { opCh == 'O' || opCh == 'o' }) {
                    UNKNOWN_COMMAND
                }
            }

            State.OP_CO -> {
                error = context.moveStateIf(State.OP_CON, { opCh == 'N' || opCh == 'n' }) {
                    UNKNOWN_COMMAND
                }
            }

            State.OP_CON -> {
                error = context.moveStateIf(State.OP_CONN, { opCh == 'N' || opCh == 'n' }) {
                    UNKNOWN_COMMAND
                }
            }

            State.OP_CONN -> {
                error = context.moveStateIf(State.OP_CONNE, { opCh == 'E' || opCh == 'e' }) {
                    UNKNOWN_COMMAND
                }
            }

            State.OP_CONNE -> {
                error = context.moveStateIf(State.OP_CONNEC, { opCh == 'C' || opCh == 'c' }) {
                    UNKNOWN_COMMAND
                }
            }

            State.OP_CONNEC -> {
                error = context.moveStateIf(State.OP_CONNECT, { opCh == 'T' || opCh == 't' }) {
                    UNKNOWN_COMMAND
                }
            }

            State.OP_CONNECT -> {
                if (context.whitespaceAfterCommand == 0 && !opCh.isWhitespace()) {
                    error = UNKNOWN_COMMAND
                } else if (opCh.isWhitespace()) {
                    context.whitespaceAfterCommand++
                    // keep same state
                } else {
                    context.state = State.OP_CONNECT_ARG
                    context.whitespaceAfterCommand = 0

                    if (context.argsBuffer != null) {
                        context.argsBuffer!!.clear()
                    } else {
                        context.argsBuffer = byteBuffFactory(options.maxArgSize)
                    }

                    context.argsBuffer!!.put(b)
                }
            }

            State.OP_CONNECT_ARG -> {
                when {
                    opCh == CRLF_BEGIN && context.carriageReturnEncountered -> error = UNKNOWN_COMMAND
                    opCh == CRLF_BEGIN -> context.carriageReturnEncountered = true
                    opCh == CRLF_END -> {
                        context.argsBuffer!!.flip()
                        operation = ConnectOperation(context.argsBuffer!!)
                    }
                    else -> {
                        if (context.argsBuffer!!.capacity() == context.argsBuffer!!.position()) {
                            error = MAX_ARG_SIZE_EXCEEDED
                        } else {
                            context.argsBuffer!!.put(b)
                        }
                    }
                }
            }

            else -> {}
        }

        if (error != null) {
            return error.left()
        } else if (operation != null) {
            // if there are other commands left, compact buffer for later processing
            bytes.limit(bytes.position()).position(bytesRead).compact()

            return operation.right()
        }
    }

    return UnparsedOperation.right()
}
