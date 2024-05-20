package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.MessageResponse
import io.github.akmal2409.knats.server.PublishRequest
import io.github.akmal2409.knats.server.SubscribeRequest
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class PubSubIntegrationTest : BaseIntegrationTest() {


    @Test
    fun `Fans out message to multiple consumers`() = runTest(super.testCoroutineConfig) {
        val firstConsumerResponseFlow = super.connectByPassInit(
            MutableStateFlow(SubscribeRequest("topic", "1"))
        )
        val secondConsumerResponseFlow = super.connectByPassInit(
            MutableStateFlow(SubscribeRequest("topic", "2"))
        )

        val firstConsumerChannel = firstConsumerResponseFlow.produceIn(backgroundScope)
        val secondConsumerChannel = secondConsumerResponseFlow.produceIn(backgroundScope)

        val firstPayload = ByteBuffer.wrap("test".toByteArray(Charsets.US_ASCII))
        val expectedFirstMessageFirstConsumer = expectedMessageForConsumer(
            "topic", "1",
            firstPayload.slice(), 4
        )
        val expectedFirstMessageSecondConsumer = expectedMessageForConsumer(
            "topic", "2",
            firstPayload.slice(), 4
        )

        val secondPayload = ByteBuffer.wrap("test2".toByteArray(Charsets.US_ASCII))
        val expectedSecondMessageFirstConsumer = expectedMessageForConsumer(
            "topic", "1",
            secondPayload.slice(), 5
        )
        val expectedSecondMessageSecondConsumer = expectedMessageForConsumer(
            "topic", "2",
            secondPayload.slice(), 5
        )

        val publisherRequestFlow = MutableStateFlow(
            PublishRequest(
                "topic", 4, firstPayload
            )
        )

        val publishJob = super.connectByPassInit(publisherRequestFlow).launchIn(backgroundScope)

        firstConsumerChannel.receive() shouldBe expectedFirstMessageFirstConsumer
        secondConsumerChannel.receive() shouldBe expectedFirstMessageSecondConsumer

        publisherRequestFlow.value =  PublishRequest(
            "topic", 5, secondPayload
        )

        firstConsumerChannel.receive() shouldBe expectedSecondMessageFirstConsumer
        secondConsumerChannel.receive() shouldBe expectedSecondMessageSecondConsumer

        secondConsumerChannel.cancel()
        firstConsumerChannel.cancel()
        publishJob.cancel()
    }

    private fun expectedMessageForConsumer(
        subject: String,
        subId: String,
        payload: ByteBuffer,
        size: Int
    ) =
        MessageResponse(
            subject, subId, size, payload
        )
}
