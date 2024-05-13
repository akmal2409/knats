package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.Request
import io.github.akmal2409.knats.server.Response
import io.github.akmal2409.knats.server.ServiceConnectionHandler
import io.github.akmal2409.knats.server.TestConfiguration
import io.github.akmal2409.knats.server.TrieClientSubjectRegistry
import io.github.akmal2409.knats.transport.ClientRequest
import java.net.InetAddress
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent

abstract class BaseIntegrationTest {

    val config = TestConfiguration()

    private val serviceConnectionHandler = ServiceConnectionHandler(config, TrieClientSubjectRegistry())
    val testCoroutineConfig = config.coroutineContext

    fun connect(requestFlow: Flow<Request>): Flow<Response> =
        serviceConnectionHandler.onConnect(ClientRequest(requestFlow, InetAddress.getLocalHost()))
            .map { it.convertResponse() }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun TestScope.ignoreAndRunCurrent() = try {
        runCurrent()
    } catch (ex: Throwable) {
        //ignore
    }
}

