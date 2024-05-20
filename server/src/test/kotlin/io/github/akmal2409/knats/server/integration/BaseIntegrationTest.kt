package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.ClientSubjectRegistry
import io.github.akmal2409.knats.server.ConnectRequest
import io.github.akmal2409.knats.server.PingResponse
import io.github.akmal2409.knats.server.Request
import io.github.akmal2409.knats.server.Response
import io.github.akmal2409.knats.server.ServiceConnectionHandler
import io.github.akmal2409.knats.server.TestConfiguration
import io.github.akmal2409.knats.server.TrieClientSubjectRegistry
import io.github.akmal2409.knats.transport.ClientRequest
import java.net.InetAddress
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent

abstract class BaseIntegrationTest {

    val config = TestConfiguration()

    protected val clientSubjectRegistry: ClientSubjectRegistry<String> = TrieClientSubjectRegistry()
    private val serviceConnectionHandler = ServiceConnectionHandler(config, clientSubjectRegistry)
    val testCoroutineConfig = config.coroutineContext

    fun connect(requestFlow: Flow<Request>): Flow<Response> =
        serviceConnectionHandler.onConnect(ClientRequest(requestFlow, InetAddress.getLocalHost()))
            .map { it.convertResponse() }


    // Connects bypassing CONNECT message, skipping INFO, dropping PING
    fun connectByPassInit(originalRequestFlow: Flow<Request>): Flow<Response> {
        val decoratedRequestFlow = flow {
            emit(ConnectRequest(verbose = false))
            originalRequestFlow.collect {

                emit(it)
            }
        }

        return connect(decoratedRequestFlow)
            .drop(1) // drops INFO
            .filter { it !is PingResponse }
    }
}

