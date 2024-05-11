package io.github.akmal2409.knats.extensions

import java.nio.channels.Channel

/**
 * Closes the underlying channel ignoring any exception
 */
fun Channel.closeAndIgnore() = try {
    this.close()
} catch (ex: Throwable) {
    // ignore
}
