package io.github.akmal2409.knats.server.integration

import io.github.akmal2409.knats.server.Subject
import io.github.akmal2409.knats.server.SubscribeRequest
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class SubscribeIntegrationTest: BaseIntegrationTest() {

    @Test
    fun `Subscribes to topic`() = runTest(super.testCoroutineConfig) {
        val requestFlow = MutableStateFlow(SubscribeRequest("test", "1"))

        val responseCollectJob = super.connectByPassInit(requestFlow)
            .launchIn(this)

        val clients = super.clientSubjectRegistry
            .clientsForSubject(Subject.fromString("test"))

        clients.shouldHaveSize(1)

        clients.first().subscriptionId shouldBe "1"

        responseCollectJob.cancel()
    }

    @Test
    fun `Supports mutliple topic subscriptions`() = runTest(super.testCoroutineConfig) {
        val requestFlow = MutableStateFlow(SubscribeRequest("test", "1"))

        val responseCollectJob = super.connectByPassInit(requestFlow)
            .launchIn(this)

        requestFlow.value = SubscribeRequest("another.sub", "2")

        val clients = super.clientSubjectRegistry
            .clientsForSubject(Subject.fromString("test")) +
                super.clientSubjectRegistry
                    .clientsForSubject(Subject.fromString("another.sub"))

        clients shouldHaveSize 2

        clients.map { it.subscriptionId }.toSet() shouldContainExactlyInAnyOrder setOf("1", "2")

        responseCollectJob.cancel()
    }

}
