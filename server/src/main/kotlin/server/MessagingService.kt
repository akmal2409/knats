package io.github.akmal2409.nats.server.server

import kotlinx.coroutines.flow.Flow

class MessagingService : ConnectionHandler<Request, Response> {

    override fun onConnect(request: ClientRequest<Request>): Flow<Response> {
        TODO("Not yet implemented")
    }
}
