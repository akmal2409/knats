package io.github.akmal2409.knats.server.common

import java.lang.IllegalArgumentException

/**
 * Convenience method that throws illegal argument exception
 */
inline fun argumentError(message: () -> String): Nothing = throw IllegalArgumentException(message())
