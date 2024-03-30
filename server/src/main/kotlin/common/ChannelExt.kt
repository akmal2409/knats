package io.github.akmal2409.nats.server.common

import java.nio.channels.Channel

/**
 * Closes the underlying channel ignoring any exception
 */
fun Channel.closeAndIgnore() = try {
    this.close()
} catch (ex: Throwable) {
    // ignore
}
