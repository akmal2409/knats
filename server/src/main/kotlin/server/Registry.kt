package io.github.akmal2409.nats.server.server

import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.channels.SocketChannel
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

private val clientRegistryLogger = KotlinLogging.logger(ClientRegistry::class.java.name)

data class ClientKey(
    val id: UUID,
    val remoteAddress: String
) {

    companion object {

        fun fromRemoteAddress(remote: String) = ClientKey(UUID.randomUUID(), remote)
    }

    override fun toString() = "$id|$remoteAddress"
}

data class Client<IN, OUT>(
    val key: ClientKey,
    val requestChannel: Channel<IN>,
    val responseChannel: ReceiveChannel<OUT>,
    val socketChannel: SocketChannel,
    val bufferedMessageCount: AtomicInteger = AtomicInteger(0),
)

interface ClientRegistry<IN, OUT> {

    fun remove(clientKey: ClientKey): Client<IN, OUT>?

    operator fun get(clientKey: ClientKey): Client<IN, OUT>?

    operator fun set(clientKey: ClientKey, client: Client<IN, OUT>)
}

fun <IN, OUT> inMemoryClientRegistry(): ClientRegistry<IN, OUT> = InMemoryClientRegistry()

private class InMemoryClientRegistry<IN, OUT> : ClientRegistry<IN, OUT> {
    private val connectedClients = ConcurrentHashMap<ClientKey, Client<IN, OUT>>()

    override fun set(clientKey: ClientKey, client: Client<IN, OUT>) {
        clientRegistryLogger.debug { "Added new client $client" }
        connectedClients[client.key] = client
    }

    override fun remove(clientKey: ClientKey) = connectedClients.remove(clientKey).apply {
        this?.let { clientRegistryLogger.debug { "Removed client $clientKey" } }
    }

    override fun get(clientKey: ClientKey) = connectedClients[clientKey]
}
