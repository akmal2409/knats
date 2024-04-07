package io.github.akmal2409.nats.transport

import java.nio.ByteBuffer
import kotlinx.coroutines.flow.Flow

fun interface ConnectionHandler<IN, OUT> {
    fun onConnect(request: ClientRequest<IN>): Flow<OUT>
}

sealed interface DeserializationResult<T> {

    companion object {
        fun <T : Any> of(value: T?) = value?.let { AvailableResult(it) }
            ?: PendingResult()
    }

    data class AvailableResult<T>(val value: T) : DeserializationResult<T>

    class PendingResult<T> : DeserializationResult<T>
}

interface RequestDeserializer<OUT> : AutoCloseable {
    fun deserialize(bytes: ByteBuffer): DeserializationResult<OUT>
}


fun interface ResponseSerializer<IN> {
    fun serialize(value: IN): ByteBuffer
}
