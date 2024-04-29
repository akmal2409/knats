package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.transport.ClientRequest
import io.github.akmal2409.knats.transport.ConnectionHandler
import io.github.akmal2409.knats.transport.common.scopedFlow
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer
import java.util.UUID
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select

private class ClientState(
    val traceId: String = "session/${UUID.randomUUID()}",
    var receivedConnect: Boolean = false,
    var options: ConnectRequest? = null
)

class ServiceConnectionHandler(
    private val config: Configuration
) : ConnectionHandler<Request, ByteBuffer> {

    private val logger = KotlinLogging.logger {}

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun onConnect(request: ClientRequest<Request>): Flow<ByteBuffer> {
        logger.info { "Received request in handler ${request.remoteAddress}" }
        val clientState = ClientState()

        return scopedFlow<ByteBuffer> { requestScope ->
            val channel = request.requestFlow.produceIn(requestScope)

            while (true) {

                select {
                    channel.onReceive {
                        logger.info { "traceId=${clientState.traceId} Received request $request" }

                        onRequestReceived(it, clientState)
                    }

                    if (!clientState.receivedConnect) {
                        onTimeout(config.connectTimeout) {
                            logger.debug { "traceId=${clientState.traceId} Authentication timeout reached" }
                            emit(convertToResponse(ErrorResponse.authenticationTimeout()))
                            throw CancellationException("Authentication timeout reached")
                        }
                    }
                }


            }
        }.catch { ex ->
            logger.error(ex) { "Error in flow" }
            throw ex
        }.onEach { logger.info { "Emitting response $it" } }
    }

    private suspend fun FlowCollector<ByteBuffer>.onRequestReceived(
        request: Request,
        state: ClientState
    ) {
        if (request !is ConnectRequest && !state.receivedConnect) {
            error(
                "traceId=${state.traceId} Received request " +
                        "other than connect on first interaction"
            )
        }


        when (request) {
            is ConnectRequest -> handleConnectRequest(request, state)
            is PongRequest -> emit(convertToResponse(PingResponse))
            else -> error("Unimplemented request method ${request::class.qualifiedName}")
        }
    }

    private suspend fun FlowCollector<ByteBuffer>.handleConnectRequest(
        request: ConnectRequest,
        state: ClientState
    ) {
        require(!state.receivedConnect) { "Connect has already been performed traceId=${state.traceId}" }

        state.receivedConnect = true
        state.options = request

        if (state.options?.verbose == true) {
            emit(convertToResponse(OkResponse))
        }
    }
}
