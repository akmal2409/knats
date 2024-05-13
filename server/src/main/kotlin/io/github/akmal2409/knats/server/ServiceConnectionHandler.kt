package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.extensions.scopedFlow
import io.github.akmal2409.knats.transport.ClientKey
import io.github.akmal2409.knats.transport.ClientRequest
import io.github.akmal2409.knats.transport.ConnectionHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlin.time.toJavaDuration

private data class ClientState(
    val clock: Clock,
    val traceId: String = "session/${UUID.randomUUID()}",
    var receivedConnect: Boolean = false,
    var options: ConnectRequest? = null,
    var lastRequestTime: Instant = Instant.now(clock),
    var pendingPingMessageSentAt: Instant? = null
) {

    fun updateLastRequestTime() {
        lastRequestTime = Instant.now(clock)
    }
}

private class SubscriptionState(
    var pingChannel: ReceiveChannel<PingResponse>? = null,
    var messageChannel: ReceiveChannel<ByteBuffer>? = null
) : AutoCloseable {
    override fun close() {
        pingChannel?.cancel(CancellationException("Closing subscriptions"))
    }
}

class ServiceConnectionHandler(
    private val config: Configuration,
    private val subjectRegistry: ClientSubjectRegistry<String>
) : ConnectionHandler<Request, ByteBuffer> {

    private val logger = KotlinLogging.logger {}

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun onConnect(request: ClientRequest<Request>): Flow<ByteBuffer> {
        logger.info { "Received request in handler ${request.remoteAddress}" }
        val clientState = ClientState(config.clock)
        val subscriptionState = SubscriptionState()

        return scopedFlow<ByteBuffer> { requestScope ->
            val channel = request.requestFlow.produceIn(requestScope)

            while (true) {

                select {
                    channel.onReceive {
                        logger.info { "traceId=${clientState.traceId} Received request $request" }

                        onRequestReceived(it, clientState, subscriptionState, requestScope)
                    }

                    if (!clientState.receivedConnect) {
                        onTimeout(config.connectTimeout) {
                            logger.debug { "traceId=${clientState.traceId} Authentication timeout reached" }
                            emit(convertToResponse(ErrorResponse.authenticationTimeout()))
                            throw CancellationException("Authentication timeout reached")
                        }
                    } else {
                        subscriptionState.pingChannel?.onReceiveCatching { channelResult ->
                            channelResult.exceptionOrNull()?.let {
                                emit(convertToResponse(ErrorResponse.staleConnection()))
                                throw CancellationException("Stale connection, no pong response or activity")
                            }
                            logger.debug { "traceId=${clientState.traceId} Sending ping message" }
                            emit(convertToResponse(channelResult.getOrThrow()))
                        }

                        subscriptionState.messageChannel?.onReceiveCatching { channelResult ->
                            logger.debug { "traceId=${clientState.traceId} Sending message" }
                            emit(channelResult.getOrThrow())
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
        state: ClientState,
        subscriptionState: SubscriptionState,
        scope: CoroutineScope
    ) {
        if (request !is ConnectRequest && !state.receivedConnect) {
            error(
                "traceId=${state.traceId} Received request " +
                        "other than connect on first interaction"
            )
        }


        when (request) {
            is ConnectRequest -> handleConnectRequest(request, state, subscriptionState, scope)
            is PongRequest -> handlePongRequest(state)
            is SubscribeRequest -> handleSubscribeRequest(request, state, subscriptionState)
            is PublishRequest -> handlePublishRequest(request, state)
        }

        state.updateLastRequestTime()
    }

    private val channelMap: MutableMap<String, Channel<ByteBuffer>> = ConcurrentHashMap()

    private suspend fun FlowCollector<ByteBuffer>.handleSubscribeRequest(
        subscribeRequest: SubscribeRequest,
        clientState: ClientState,
        subscriptionState: SubscriptionState
    ) {
        val subject = Subject.fromString(subscribeRequest.subject)

        subjectRegistry.add(clientState.traceId, subject)

        val channel = channelMap[clientState.traceId] ?: Channel()
        channelMap[clientState.traceId] = channel

        subscriptionState.messageChannel = channel

        if (clientState.options?.verbose == true) {
            emit(convertToResponse(OkResponse))
        }
    }

    private suspend fun FlowCollector<ByteBuffer>.handlePublishRequest(
        publishRequest: PublishRequest,
        clientState: ClientState
    ) {

        val subject = Subject.fromString(publishRequest.subject)
        val clients = subjectRegistry.clientsForSubject(subject)

        val buffer = ByteBuffer.allocate(publishRequest.payload.capacity() + 2)
        buffer.put(publishRequest.payload)
        buffer.put(publishRequest.payload.capacity(), '\r'.toByte())
        buffer.put(publishRequest.payload.capacity() + 1, '\n'.toByte())
        buffer.position(0)
        buffer.limit(buffer.capacity())

        clients.forEach { client ->
            logger.info { "Going to fanout to client=$client" }
            channelMap[client]?.send(buffer.slice().asReadOnlyBuffer())
        }

        if (clientState.options?.verbose == true) {
            emit(convertToResponse(OkResponse))
        }
    }

    private suspend fun FlowCollector<ByteBuffer>.handleConnectRequest(
        request: ConnectRequest,
        state: ClientState,
        subscriptionState: SubscriptionState,
        scope: CoroutineScope
    ) {
        require(!state.receivedConnect) { "Connect has already been performed traceId=${state.traceId}" }

        state.receivedConnect = true
        state.options = request

        if (state.options?.verbose == true) {
            emit(convertToResponse(OkResponse))
        }

        subscriptionState.pingChannel = createPingFlow(state).produceIn(scope)
    }

    private fun handlePongRequest(state: ClientState) {

        logger.debug {
            "traceId=${state.traceId} Ping received after=${
                Duration.between(
                    state.pendingPingMessageSentAt,
                    Instant.now(config.clock)
                ).toMillis()
            }ms"
        }
        state.updateLastRequestTime()
    }

    private fun createPingFlow(state: ClientState): Flow<PingResponse> = flow {

        while (true) {
            val pingSentAt = state.pendingPingMessageSentAt
            val pingTimeoutAt = pingSentAt?.let { pingSentAt + config.pingTimeout.toJavaDuration() }
            val now = Instant.now(config.clock)

            val nextPingAt = state.lastRequestTime + config.pingAfterInactivity.toJavaDuration()

            when {
                pingSentAt != null && pingSentAt < state.lastRequestTime -> {
                    // client, interacted with us, do not expect a ping
                    state.pendingPingMessageSentAt = null
                }

                pingTimeoutAt != null && pingTimeoutAt <= now -> {
                    error("Did not receive ping response within allotted period")
                }

                pingSentAt == null && nextPingAt <= now -> {
                    emit(PingResponse)
                    state.pendingPingMessageSentAt = now
                }
            }

            val updatedPingSentAt = state.pendingPingMessageSentAt
            if (updatedPingSentAt != null) {
                val timeoutAt = (updatedPingSentAt + config.pingTimeout.toJavaDuration())
                delay(Duration.between(now, timeoutAt).toMillis())
            } else {
                delay(Duration.between(now, nextPingAt).toMillis())
            }
        }
    }
}
