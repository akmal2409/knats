package io.github.akmal2409.knats.server

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestDispatcher
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
}

data class RealConfiguration(
    override val host: String = "localhost",
    override val port: Int = 5000,
    override val connectTimeout: Duration = Duration.parse("30s"),
    override val maxPayloadSize: Int = 10 * 1024 * 1024,
    override val maxArgSize: Int = 1024,
    override val coroutineContext: CoroutineContext = Dispatchers.Default
) : Configuration

data class TestConfiguration(
    override val host: String = "localhost",
    override val port: Int = 5000,
    override val connectTimeout: Duration = Duration.parse("30s"),
    override val maxPayloadSize: Int = 10 * 1024 * 1024,
    override val maxArgSize: Int = 1024,
    override val coroutineContext: CoroutineContext = TestCoroutineScheduler()
) : Configuration
