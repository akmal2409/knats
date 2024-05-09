package io.github.akmal2409.knats.server

import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration

/**
 * @param connectTimeout specifies the duration after which,
 *  if connect request was not received, the connection is closed
 */
interface Configuration {
    val host: String
    val port: Int
    val connectTimeout: Duration
    val maxPayloadSize: Int
    val maxArgSize: Int
    val coroutineContext: CoroutineContext
    val clock: Clock
    val pingAfterInactivity: Duration
    val pingTimeout: Duration
}

data class RealConfiguration(
    override val host: String = "localhost",
    override val port: Int = 5000,
    override val connectTimeout: Duration = Duration.parse("30s"),
    override val maxPayloadSize: Int = 10 * 1024 * 1024,
    override val maxArgSize: Int = 1024,
    override val coroutineContext: CoroutineContext = Dispatchers.Default,
    override val clock: Clock = Clock.systemUTC(),
    override val pingAfterInactivity: Duration = Duration.parse("5s"),
    override val pingTimeout: Duration = Duration.parse("8s")
) : Configuration

data class TestConfiguration @OptIn(ExperimentalCoroutinesApi::class) constructor(
    override val host: String = "localhost",
    override val port: Int = 5000,
    override val connectTimeout: Duration = Duration.parse("30s"),
    override val maxPayloadSize: Int = 10 * 1024 * 1024,
    override val maxArgSize: Int = 1024,
    override val coroutineContext: TestDispatcher = UnconfinedTestDispatcher(),
    override val clock: Clock = TestClock(coroutineContext.scheduler),
    override val pingAfterInactivity: Duration = Duration.parse("10s"),
    override val pingTimeout: Duration = Duration.parse("20s")
) : Configuration

class TestClock(private val testContext: TestCoroutineScheduler) : Clock() {
    @OptIn(ExperimentalCoroutinesApi::class)
    override fun instant(): Instant = Instant.ofEpochMilli(testContext.currentTime)

    override fun withZone(zone: ZoneId?) = this

    override fun getZone(): ZoneId = ZoneOffset.UTC
}
