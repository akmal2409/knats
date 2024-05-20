package io.github.akmal2409.knats.server.common

import kotlin.math.abs
import kotlin.math.log10

/**
 * Returns number of digits in [this] integer
 */
fun Int.numberOfDigits(): Int = when(val absolute = abs(this)) {
    0, 1 -> 1 // logx(0) == INF, logx(1) == 0
    else -> log10(absolute.toFloat()).toInt() + 1
}
