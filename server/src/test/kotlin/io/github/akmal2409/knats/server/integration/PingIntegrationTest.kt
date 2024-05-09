package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.ConnectRequest
import io.github.akmal2409.knats.server.PingResponse
import io.github.akmal2409.knats.server.PongRequest
import io.github.akmal2409.knats.server.Request
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import java.util.concurrent.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.time.DurationUnit

class PingIntegrationTest : BaseIntegrationTest() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `Sends PING after inactivity`() = runTest(super.testCoroutineConfig) {
        val requestFlow = MutableStateFlow<Request>(ConnectRequest(verbose = false))

        val responseFlow = super.connect(requestFlow)
        val responseChannel = responseFlow.produceIn(this)
        val connectTime = currentTime

        responseChannel.isEmpty.shouldBeTrue()

        val result = responseChannel.receive()

        result.shouldNotBeNull()
        result shouldBe PingResponse
        currentTime shouldBeGreaterThanOrEqual connectTime + config.pingAfterInactivity.toLong(
            DurationUnit.MILLISECONDS
        )
        responseChannel.cancel(CancellationException())
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `Closes connection if PING not received`() = runTest(super.testCoroutineConfig) {
        val requestFlow = MutableStateFlow<Request>(ConnectRequest(verbose = false))
        val responseFlow = super.connect(requestFlow)
        var ex: Throwable? = null

        responseFlow
            .catch { ex = it }
            .launchIn(this)

        advanceTimeBy(config.pingAfterInactivity + config.pingTimeout)
        runCurrent()

        ex.shouldBeInstanceOf<IllegalStateException>()
    }

    @Test
    @OptIn(ExperimentalCoroutinesApi::class)
    fun `Does not close connection if PONG received`() = runTest(super.testCoroutineConfig) {
        val requestFlow = MutableStateFlow<Request>(ConnectRequest(verbose = false))
        val responseFlow = super.connect(requestFlow)
        val responseChannel = responseFlow.produceIn(this)

        advanceTimeBy(config.pingAfterInactivity)

        responseChannel.receive() shouldBe PingResponse

        requestFlow.value = PongRequest
        advanceTimeBy(config.pingTimeout)

        val result = responseChannel.tryReceive()
        result.exceptionOrNull().shouldBeNull()
        responseChannel.cancel()
    }
}
