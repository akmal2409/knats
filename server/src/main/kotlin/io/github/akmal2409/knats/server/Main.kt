package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.server.parser.ParsingError
import io.github.akmal2409.knats.server.parser.PendingParsing
import io.github.akmal2409.knats.server.parser.SuspendableParser
import io.github.akmal2409.knats.transport.ClientRequest
import io.github.akmal2409.knats.transport.ConnectionHandler
import io.github.akmal2409.knats.transport.DeserializationResult
import io.github.akmal2409.knats.transport.NioTcpServerSocketChannelFactory
import io.github.akmal2409.knats.transport.RequestDeserializer
import io.github.akmal2409.knats.transport.SelectorFactoryImpl
import io.github.akmal2409.knats.transport.Transport
import io.github.akmal2409.knats.transport.common.scopedFlow
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select

class ScheduledPing(
    val timeoutReached: Boolean = false
)

object Handler : ConnectionHandler<Request, ByteBuffer> {

    private val logger = KotlinLogging.logger {}

    override fun onConnect(request: ClientRequest<Request>): Flow<ByteBuffer> {
        logger.info { "Received request in handler ${request.remoteAddress}" }

        return scopedFlow<ByteBuffer> { requestScope ->
            val channel = request.requestFlow.produceIn(requestScope)

            while (true) {

                select {
                    channel.onReceive {
                        logger.info { "Received request $request" }

                        emit(convertToResponse(PingResponse))
                    }
                }


            }
        }.catch { ex ->
            logger.error(ex) { "Error in flow" }
            throw ex
        }.onEach { logger.info { "Emitting response $it" } }
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
