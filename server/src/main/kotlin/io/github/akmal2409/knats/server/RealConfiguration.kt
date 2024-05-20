package io.github.akmal2409.knats.server

import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID
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
    val serverId: String
    val serverName: String
    val version: String
    val goVersion: String
    val host: String
    val advertisedHost: String
    val protocolVersion: Int
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
    override val maxArgSize: Int = 10 * 1024,
    override val coroutineContext: CoroutineContext = Dispatchers.Default,
    override val clock: Clock = Clock.systemUTC(),
    override val pingAfterInactivity: Duration = Duration.parse("10m"),
    override val pingTimeout: Duration = Duration.parse("30s"),
    override val advertisedHost: String = host,
    override val goVersion: String = "1.11",
    override val protocolVersion: Int = 1,
    override val serverId: String = UUID.randomUUID().toString(),
    override val serverName: String = "knats",
    override val version: String = "1.30"
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
    override val pingTimeout: Duration = Duration.parse("20s"),
    override val advertisedHost: String = host,
    override val goVersion: String = "1.11",
    override val protocolVersion: Int = 1,
    override val serverId: String = "knats-test-1",
    override val serverName: String = "knats-test",
    override val version: String = "1.30"
) : Configuration

class TestClock(private val testContext: TestCoroutineScheduler) : Clock() {
    @OptIn(ExperimentalCoroutinesApi::class)
    override fun instant(): Instant = Instant.ofEpochMilli(testContext.currentTime)

    override fun withZone(zone: ZoneId?) = this

    override fun getZone(): ZoneId = ZoneOffset.UTC
}
