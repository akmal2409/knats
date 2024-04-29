package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.ConnectRequest
import io.github.akmal2409.knats.server.ErrorResponse
import io.github.akmal2409.knats.server.OkResponse
import io.github.akmal2409.knats.server.PongRequest
import io.github.akmal2409.knats.server.Request
import io.github.akmal2409.knats.server.Response
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.time.Duration

class ConnectIntegrationTest : BaseIntegrationTest() {

    @Test
    fun `Disconnects with an error on connection timeout`() = runTest(super.testCoroutineConfig) {
        val requestFlow = flow<Request> {
            delay(config.connectTimeout + Duration.parse("10s"))
        }

        val responseFlow = super.connect(requestFlow)

        var response: Response? = null

        val ex: Throwable? = responseFlow.collectWithException {
            response = it
        }

        ex.shouldNotBeNull()
        ex.shouldBeInstanceOf<CancellationException>()

        response.shouldNotBeNull()
        response shouldBe ErrorResponse.authenticationTimeout()
    }

    @Test
    fun `Responds with +OK on Connect when verbose is on`() = runTest(super.testCoroutineConfig) {
        val requestFlow = MutableStateFlow(ConnectRequest(verbose = true))

        val responseFlow = super.connect(requestFlow)

        val response = responseFlow.take(1).single()

        response shouldBe OkResponse
    }

    @Test
    fun `Throws error if any other command other than CONNECT is first`() = runTest(super.testCoroutineConfig) {
        val requestFlow = MutableStateFlow(PongRequest)

        val responseFlow = super.connect(requestFlow)

        val ex = responseFlow.collectWithException {
            error("Should not have emitted any value")
        }

        ex.shouldBeInstanceOf<IllegalStateException>()
    }
}
