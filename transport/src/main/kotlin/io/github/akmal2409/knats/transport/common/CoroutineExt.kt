package io.github.akmal2409.knats.transport.common

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow

fun <T> scopedFlow(block: suspend FlowCollector<T>.(CoroutineScope) -> Unit) = flow<T> {
    coroutineScope scope@{
        this@flow.block(this@scope)
    }
}
