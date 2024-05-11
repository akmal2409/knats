package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.server.util.createArgsBuffer
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import kotlin.test.Test


class RequestConvertersTest {


    @Test
    fun `SubscribeRequest fromArgsBuffer maps when subId, id and queue group present`() {
        val argsBuffer = createArgsBuffer("subject", "group", "id")

        val request = SubscribeRequest.fromArgsBuffer(argsBuffer)

        request.subject shouldBe "subject"
        request.queueGroup shouldBe "group"
        request.subscriptionId shouldBe "id"
    }

    @Test
    fun `SubscribeRequest fromArgsBuffer maps when subId and id is present`() {
        val argsBuffer = createArgsBuffer("subject", "id")
        val request = SubscribeRequest.fromArgsBuffer(argsBuffer)

        request.subject shouldBe "subject"
        request.queueGroup.shouldBeNull()
        request.subscriptionId shouldBe "id"
    }
}
