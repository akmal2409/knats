package io.github.akmal2409.nats.server

import io.github.akmal2409.nats.transport.ClientRequest
import io.github.akmal2409.nats.transport.ConnectionHandler
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow

class MessageService(
    private val dispatcher: CoroutineDispatcher
) : ConnectionHandler<Request, Response> {

    private val serviceScope = CoroutineScope(dispatcher)

    override fun onConnect(request: ClientRequest<Request>): Flow<Response> {
        TODO("Not yet implemented")
    }
}
