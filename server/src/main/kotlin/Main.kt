package io.github.akmal2409.nats.server

import io.github.akmal2409.nats.server.common.scopedFlow
import io.github.akmal2409.nats.server.parser.SuspendableParser
import io.github.akmal2409.nats.server.server.ClientRequest
import io.github.akmal2409.nats.server.server.ConnectionHandler
import io.github.akmal2409.nats.server.server.DeserializationResult
import io.github.akmal2409.nats.server.server.NioTcpServerSocketChannelFactory
import io.github.akmal2409.nats.server.server.PingResponse
import io.github.akmal2409.nats.server.server.Request
import io.github.akmal2409.nats.server.server.RequestDeserializer
import io.github.akmal2409.nats.server.server.SelectorFactoryImpl
import io.github.akmal2409.nats.server.server.Transport
import io.github.akmal2409.nats.server.server.convertToRequest
import io.github.akmal2409.nats.server.server.convertToResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.time.Instant
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import parser.ParsingError
import parser.PendingParsing

class ScheduledPing(
    val timeoutReached: Boolean = false
)

object Handler : ConnectionHandler<Request, ByteBuffer> {

    private val logger = KotlinLogging.logger {}

    private val scope = CoroutineScope(Dispatchers.Default)

    override fun onConnect(request: ClientRequest<Request>): Flow<ByteBuffer> {
        logger.info { "Received request in handler ${request.remoteAddress}" }

        return scopedFlow<ByteBuffer> { requestScope ->
            val channel = request.requestFlow.produceIn(requestScope)
            val pingChannel = pingFlow().produceIn(requestScope)

            while (true) {

                select {
                    channel.onReceive {
                        logger.info { "Received request $request" }

                        emit(convertToResponse(PingResponse))
                    }

//                    pingChannel.onReceive {
//                        if (it.timeoutReached) {
//                            logger.info { "Timeout reached for client. Terminating connection" }
//                            throw RuntimeException("Timeout reached")
//                        } else {
//                            logger.info { "Pinging client" }
//                            emit(convertToResponse(PingResponse))
//                        }
//                    }
                }


            }
        }.catch { ex ->
            logger.error(ex) { "Error in flow" }
            throw ex
        }.onEach { logger.info { "Emitting response $it" } }
    }


    private fun pingFlow() = flow<ScheduledPing> {
        var lastPing: Instant = Instant.EPOCH

        while (true) {
            val now = Instant.now()

            if (lastPing != Instant.EPOCH && now.isAfter(lastPing.plusSeconds(4))) {
                emit(ScheduledPing(timeoutReached = true))
                delay(12000)
            } else {
                lastPing = Instant.now()
                emit(ScheduledPing())
                delay(5000)
            }
        }
    }
}

class Deser : RequestDeserializer<Request> {

    private val parser = SuspendableParser(10 * 1024 * 1024, 1024)

    override fun deserialize(bytes: ByteBuffer): DeserializationResult<Request> {
        return when (val result = parser.tryParse(bytes)) {
            is PendingParsing -> DeserializationResult.of(null)
            is ParsingError -> error("Parsing error $result")
            else -> DeserializationResult.of(convertToRequest(result))
        }
    }

    override fun close() {
        parser.close()
    }
}

fun main() = runBlocking {
    val server = Transport(
        serverSocketChannelFactory = NioTcpServerSocketChannelFactory(
            InetSocketAddress("localhost", 5000)
        ),
        selectorFactory = SelectorFactoryImpl(),
        connectionHandler = Handler,
        requestDeserializerFactory = { Deser() },
        responseSerializer = { it }
    )

    server.start()

    Runtime.getRuntime().addShutdownHook(Thread(server::stop))
}
