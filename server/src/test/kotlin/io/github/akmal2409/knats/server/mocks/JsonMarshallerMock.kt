package io.github.akmal2409.knats.server.mocks

import io.github.akmal2409.knats.server.json.JsonMarshaller
import io.github.akmal2409.knats.server.json.JsonValue

class JsonMarshallerMock(
    var result: Map<String, JsonValue>,
    var ex: Throwable? = null
) : JsonMarshaller {

    var inputJsons: MutableList<String> = mutableListOf()

    override fun unmarshall(jsonStr: String): Map<String, JsonValue> {
        inputJsons.add(jsonStr)

        return ex?.let { throw it } ?: result
    }
}
