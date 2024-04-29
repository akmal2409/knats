package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.Request
import io.github.akmal2409.knats.server.Response
import io.github.akmal2409.knats.server.ServiceConnectionHandler
import io.github.akmal2409.knats.server.TestConfiguration
import io.github.akmal2409.knats.transport.ClientRequest
import java.net.InetAddress
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.test.TestCoroutineScheduler

abstract class BaseIntegrationTest {

    val config = TestConfiguration()

    val serviceConnectionHandler = ServiceConnectionHandler(config)
    val testCoroutineConfig = TestCoroutineScheduler()

    fun connect(requestFlow: Flow<Request>): Flow<Response> =
        serviceConnectionHandler.onConnect(ClientRequest(requestFlow, InetAddress.getLocalHost()))
            .map { it.convertResponse() }
}
