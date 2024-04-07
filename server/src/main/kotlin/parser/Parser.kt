package io.github.akmal2409.nats.server.parser

import io.github.akmal2409.nats.transport.common.toAsciiChar
import java.nio.ByteBuffer
import io.github.akmal2409.nats.server.parser.ParsingError.Companion.INVALID_CLIENT_PROTOCOL
import io.github.akmal2409.nats.server.parser.ParsingError.Companion.MAXIMUM_PAYLOAD_VIOLATION


/**
 * To implement somewhat zero allocation parsing, we need to have states to define transitions
 */
private val connectOps = setOf(
    State.OP_C,
    State.OP_CO,
    State.OP_CON,
    State.OP_CONN,
    State.OP_CONNE,
    State.OP_CONNEC,
    State.OP_CONNECT,
    State.OP_CONNECT_PARSED_ARGS
)
private val subOps = setOf(
    State.OP_S, State.OP_SU, State.OP_SUB, State.OP_SUB_PARSED_ARGS
)

private val pubOps = setOf(
    State.OP_PU, State.OP_PUB, State.OP_PUB_PARSED_ARGS, State.OP_PUB_READ_PAYLOAD
)

private val pongOps = setOf(
    State.OP_PO, State.OP_PON, State.OP_PONG
)

enum class State {
    OP_START, OP_PARSE_ARGS,
    OP_C, OP_CO, OP_CON, OP_CONN, OP_CONNE, OP_CONNEC, OP_CONNECT, OP_CONNECT_PARSED_ARGS, // will follow json like conifg {}

    OP_P, OP_PO, OP_PON, OP_PONG, // no args after

    OP_S, OP_SU, OP_SUB, OP_SUB_PARSED_ARGS, // with subject, optionally queue group and sub_id after

    OP_PU, OP_PUB, OP_PUB_PARSED_ARGS, OP_PUB_READ_PAYLOAD; // with subject, optionally reply-to, byte count + payload


    val whitespaceAllowed
        get() = this == OP_CONNECT || this == OP_SUB || this == OP_PUB

    fun isConnectOp() = connectOps.contains(this)

    fun isSubOp() = subOps.contains(this)

    fun isPubOp() = pubOps.contains(this)
    fun isPongOp() = pongOps.contains(this)
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
    var parsingArgs: Boolean = false,
    var readingBody: Boolean = false,
    var stateAfterParsedArgs: State? = null,
    var argsBuffer: ByteBuffer? = null,
    var payloadBuffer: ByteBuffer? = null
) {


    companion object {
        fun initial(): Context = Context(State.OP_START)
    }

    fun moveStateOnCh(targetState: State, actualCh: Char, targetCh: Char): ParsingError? {
        var error: ParsingError? = null

        if (targetCh.equals(actualCh, true)) {
            this.state = targetState
        } else {
            error = INVALID_CLIENT_PROTOCOL
        }

        return error
    }


    fun prepareForArgParsing(afterArgParseState: State, bufferFactory: () -> ByteBuffer) {
        this.state = State.OP_PARSE_ARGS
        this.stateAfterParsedArgs = afterArgParseState
        this.whitespaceAfterCommand = 0

        this.argsBuffer = this.argsBuffer ?: bufferFactory()
        this.parsingArgs = true
    }
}

// this protocol considers only empty space and \t as a whitespace
private fun Char.isWhitespace() = this == ' ' || this == '\t'

/**
 * Implementation of a parser that can parse data from incoming streams.
 * Useful when having a NIO channel reading from socket whatever is available.
 */
class SuspendableParser(
    val maxPayloadSize: Int,
    val maxArgSize: Int,
    var bytesRead: Int = 0,
    private val bufferFactory: (Int) -> ByteBuffer = ByteBuffer::allocate,
    private val recycleBuffer: (ByteBuffer) -> Unit = {},
    private val context: Context = Context.initial()
) : AutoCloseable {

    /**
     * Clears the parser to be used in the next command sequence.
     */
    fun clear() {
        context.argsBuffer?.clear()
        context.whitespaceAfterCommand = 0
        context.state = State.OP_START
        context.carriageReturnEncountered = false
        context.parsingArgs = false
        context.readingBody = false
        bytesRead = 0
    }

    override fun close() {
        clear()
        context.argsBuffer?.let(recycleBuffer)
        context.argsBuffer = null
    }

    fun tryParse(bytes: ByteBuffer): ParsingResult {

        var result: ParsingResult? = null

        while (bytes.hasRemaining() && result == null) {
            val b = bytes.get()
            bytesRead++
            val opCh = b.toAsciiChar()

            if (context.carriageReturnEncountered
                && !context.parsingArgs
                && !context.readingBody
                && (opCh != CRLF_END || opCh == CRLF_BEGIN)
            ) {
                result = INVALID_CLIENT_PROTOCOL
                break
            }

            if (opCh.isWhitespace() && context.state.whitespaceAllowed) {
                context.whitespaceAfterCommand++
                continue
            }

            result = when {
                context.state == State.OP_START -> {
                    when {
                        'c'.equals(opCh, true) -> {
                            context.state = State.OP_C
                            null
                        }

                        's'.equals(opCh, true) -> {
                            context.state = State.OP_S
                            null
                        }

                        'p'.equals(opCh, true) -> {
                            context.state = State.OP_P
                            null
                        }

                        else -> INVALID_CLIENT_PROTOCOL
                    }
                }

                context.state == State.OP_PARSE_ARGS -> parseArgs(context, b, opCh, bytes)

                context.state == State.OP_P -> {
                    when {
                        'u'.equals(opCh, true) -> {
                            context.state = State.OP_PU
                            null
                        }

                        'o'.equals(opCh, true) -> {
                            context.state = State.OP_PO
                            null
                        }

                        else -> INVALID_CLIENT_PROTOCOL
                    }
                }

                context.state.isConnectOp() -> parseConnect(context, opCh, bytes)

                context.state.isSubOp() -> parseSub(context, opCh, bytes)

                context.state.isPubOp() -> parsePub(context, b, opCh, bytes)

                context.state.isPongOp() -> parsePong(context, opCh)


                else -> error("Unsupported global state transition")
            }
        }


        return result ?: PendingParsing(bytesRead)
    }

    private fun parsePong(context: Context, opCh: Char): ParsingResult? = when (context.state) {
        State.OP_PO -> context.moveStateOnCh(State.OP_PON, opCh, 'n')

        State.OP_PON -> context.moveStateOnCh(State.OP_PONG, opCh, 'g')

        State.OP_PONG -> when {
            opCh == CRLF_BEGIN && !context.carriageReturnEncountered -> {
                context.carriageReturnEncountered = true
                null
            }

            opCh == CRLF_END && context.carriageReturnEncountered -> {
                PongOperation()
            }

            else -> INVALID_CLIENT_PROTOCOL
        }

        else -> error("Illegal state transition for PONG ${context.state}")
    }

    private fun parseArgs(
        context: Context,
        byte: Byte,
        opCh: Char,
        bytes: ByteBuffer
    ): ParsingResult? {
        val argsBuffer = context.argsBuffer ?: error("Args buffer is null")
        val stateAfterParsing =
            context.stateAfterParsedArgs ?: error("Required state transition after parsing args")
        return when {

            opCh == CRLF_BEGIN -> {
                context.carriageReturnEncountered = true
                null
            }

            opCh == CRLF_END && context.carriageReturnEncountered -> {
                context.carriageReturnEncountered = false
                if (context.argsBuffer!!.position() == 0) {
                    INVALID_CLIENT_PROTOCOL
                } else {
                    context.state = stateAfterParsing
                    context.stateAfterParsedArgs = null
                    bytes.position(bytes.position() - 1) // not to exit the loop
                    null
                }
            }

            context.carriageReturnEncountered -> {
                context.carriageReturnEncountered = false
                argsBuffer.put(CRLF_BEGIN.code.toByte())
                argsBuffer.put(byte)
                null
            }

            else -> {
                if (context.argsBuffer!!.capacity() == context.argsBuffer!!.position()) {
                    MAXIMUM_PAYLOAD_VIOLATION
                } else {
                    argsBuffer.put(byte)
                    null
                }
            }
        }
    }

    private fun parseConnect(
        context: Context,
        opCh: Char,
        bytes: ByteBuffer
    ): ParsingResult? {
        var result: ParsingResult? = null

        when (context.state) {

            State.OP_C -> {
                result = context.moveStateOnCh(State.OP_CO, opCh, 'o')
            }

            State.OP_CO -> {
                result = context.moveStateOnCh(State.OP_CON, opCh, 'n')
            }

            State.OP_CON -> {
                result = context.moveStateOnCh(State.OP_CONN, opCh, 'n')
            }

            State.OP_CONN -> {
                result = context.moveStateOnCh(State.OP_CONNE, opCh, 'e')
            }

            State.OP_CONNE -> {
                result = context.moveStateOnCh(State.OP_CONNEC, opCh, 'c')
            }

            State.OP_CONNEC -> {
                result = context.moveStateOnCh(State.OP_CONNECT, opCh, 't')
            }

            State.OP_CONNECT -> {
                if (context.whitespaceAfterCommand == 0 && !opCh.isWhitespace()) {
                    result = INVALID_CLIENT_PROTOCOL
                } else {
                    context.prepareForArgParsing(
                        State.OP_CONNECT_PARSED_ARGS,
                        { bufferFactory(maxArgSize) })
                    bytes.position(bytes.position() - 1)
                }
            }

            State.OP_CONNECT_PARSED_ARGS -> {

                if (context.argsBuffer == null || context.argsBuffer!!.position() == 0) {
                    result = INVALID_CLIENT_PROTOCOL
                } else {
                    context.argsBuffer!!.flip()
                    result = ConnectOperation(context.argsBuffer!!)
                }
            }

            else -> error("Unsupported connect state: ${context.state}")
        }

        return result
    }

    private fun parseSub(
        context: Context,
        opCh: Char,
        bytes: ByteBuffer
    ): ParsingResult? {
        var result: ParsingResult? = null

        when (context.state) {

            State.OP_S -> {
                result = context.moveStateOnCh(State.OP_SU, opCh, 'u')
            }

            State.OP_SU -> {
                result = context.moveStateOnCh(State.OP_SUB, opCh, 'b')
            }

            State.OP_SUB -> {
                if (!opCh.isWhitespace() && context.whitespaceAfterCommand == 0) {
                    result = INVALID_CLIENT_PROTOCOL
                } else {
                    context.prepareForArgParsing(State.OP_SUB_PARSED_ARGS) {
                        bufferFactory(
                            maxArgSize
                        )
                    }
                    bytes.position(bytes.position() - 1)
                }
            }

            State.OP_SUB_PARSED_ARGS -> {
                if (context.argsBuffer == null || context.argsBuffer!!.position() == 0) {
                    result = INVALID_CLIENT_PROTOCOL
                } else {
                    context.argsBuffer!!.flip()
                    result = SubscribeOperation(context.argsBuffer!!)
                }
            }

            else -> result = INVALID_CLIENT_PROTOCOL
        }

        return result
    }

    private fun parsePub(
        context: Context,
        byte: Byte,
        opCh: Char,
        bytes: ByteBuffer
    ): ParsingResult? = when {
        context.state == State.OP_PU -> context.moveStateOnCh(
            State.OP_PUB,
            opCh, 'b'
        )

        context.state == State.OP_PUB -> {
            if (!opCh.isWhitespace() && context.whitespaceAfterCommand == 0) {
                INVALID_CLIENT_PROTOCOL
            } else {
                context.prepareForArgParsing(State.OP_PUB_PARSED_ARGS) {
                    bufferFactory(
                        maxArgSize
                    )
                }
                // to read byte again (it's part of args)
                bytes.position(bytes.position() - 1)
                null
            }
        }

        context.state == State.OP_PUB_PARSED_ARGS -> {
            if (context.argsBuffer == null || context.argsBuffer!!.position() < 3) {
                // means doesnt have an int (for payload size) and at least 1 char for subject + whitespace
                INVALID_CLIENT_PROTOCOL
            } else {
                // we need to read last bytes as int to get payload size
                context.argsBuffer!!.flip()

                val payloadSize = readAsciiNumFromEnd(context.argsBuffer!!)

                if (payloadSize == null || payloadSize < 0 || payloadSize > maxPayloadSize) {
                    MAXIMUM_PAYLOAD_VIOLATION
                } else {
                    var payloadBuffer = context.payloadBuffer ?: bufferFactory(payloadSize)

                    if (payloadBuffer.limit() < payloadSize) {
                        payloadBuffer = bufferFactory(payloadSize)
                    }

                    context.state = State.OP_PUB_READ_PAYLOAD
                    context.readingBody = true
                    context.payloadBuffer = payloadBuffer
                    null
                }
            }

        }

        context.state == State.OP_PUB_READ_PAYLOAD -> {
            val payloadBuffer = context.payloadBuffer ?: error("Payload buffer is not set")

            when {
                payloadBuffer.position() < payloadBuffer.limit() -> {
                    payloadBuffer.put(byte)
                    null
                }

                opCh == CRLF_BEGIN && !context.carriageReturnEncountered -> {
                    context.carriageReturnEncountered = true
                    null
                }

                opCh == CRLF_END && context.carriageReturnEncountered -> {
                    payloadBuffer.flip()
                    PublishOperation(context.argsBuffer!!, payloadBuffer)
                }

                else -> MAXIMUM_PAYLOAD_VIOLATION
            }
        }

        else -> INVALID_CLIENT_PROTOCOL
    }

    private fun readAsciiNumFromEnd(bytes: ByteBuffer): Int? {
        var position = bytes.limit() - 1

        while (position >= 0 && bytes[position].toAsciiChar().isDigit()) {
            position--
        }

        position++

        if (position == bytes.limit()) return null

        var num = 0

        while (position < bytes.limit()) {
            num *= 10
            num += (bytes[position].toAsciiChar() - '0')
            position++
        }

        return num
    }
}


