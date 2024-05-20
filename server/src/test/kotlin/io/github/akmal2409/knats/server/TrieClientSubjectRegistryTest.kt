package io.github.akmal2409.knats.server

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import kotlin.test.Test


@Suppress("VariableNaming")
class TrieClientSubjectRegistryTest {

    val client1_1 = ClientSubscriptionRef("client-1", "1")
    val client1_2 = ClientSubscriptionRef("client-1", "2")
    val client2_1 = ClientSubscriptionRef("client-2", "1")
    val client2_2 = ClientSubscriptionRef("client-2", "2")
    val client3_1 = ClientSubscriptionRef("client-3", "1")
    val client3_2 = ClientSubscriptionRef("client-3", "2")
    val client4_1 = ClientSubscriptionRef("client-4", "1")

    @Test
    fun `Supports plain token queries`() {
        val registry = TrieClientSubjectRegistry<String>()
        val clientSubscriptions = listOf(
            client1_1 to Subject.fromString("foo"),
            client2_1 to Subject.fromString("fooz"),
            client3_1 to Subject.fromString("foo.bar"),
            client4_1 to Subject.fromString("foo.bar.star")
        )

        registry.addClients(clientSubscriptions)

        registry.clientsForSubject(Subject.fromString("foo")) shouldContainExactlyInAnyOrder setOf(
            client1_1
        )

        registry.clientsForSubject(Subject.fromString("fooz")) shouldContainExactlyInAnyOrder setOf(
            client2_1
        )

        registry.clientsForSubject(Subject.fromString("foo.bar")) shouldContainExactlyInAnyOrder setOf(
            client3_1
        )

        registry.clientsForSubject(Subject.fromString("foo.bar.star")) shouldContainExactlyInAnyOrder setOf(
            client4_1
        )
    }

    @Test
    fun `Supports wildcard query`() {
        val registry = TrieClientSubjectRegistry<String>()
        val clientSubscriptions = listOf(
            client1_1 to Subject.fromString("foo.*"),
            client2_1 to Subject.fromString("foo.bar"),
            client3_1 to Subject.fromString("foo.bar.star"),
            client3_2 to Subject.fromString("bar.star"),
            client4_1 to Subject.fromString("foo.lol.star")
        )

        registry.addClients(clientSubscriptions)

        registry.clientsForSubject(Subject.fromString("foo.*")) shouldContainExactlyInAnyOrder
                setOf(client1_1, client2_1)

        registry.clientsForSubject(Subject.fromString("foo.*.star")) shouldContainExactlyInAnyOrder
                setOf(client3_1, client4_1)
    }

    @Test
    fun `Supports match rest query`() {
        val registry = TrieClientSubjectRegistry<String>()
        val clientSubscriptions = listOf(
            client1_1 to Subject.fromString("foo.*"),
            client1_2 to Subject.fromString("first.*.second.*.third"),
            client2_1 to Subject.fromString("foo.bar.star.lol"),
            client2_2 to Subject.fromString("first.third.*"),
            client3_1 to Subject.fromString("bar"),
            client4_1 to Subject.fromString("foo.me.star")
        )

        registry.addClients(clientSubscriptions)

        registry.clientsForSubject(Subject.fromString("foo.>")) shouldContainExactlyInAnyOrder
                setOf(client1_1, client2_1, client4_1)

        registry.clientsForSubject(Subject.fromString("first.>")) shouldContainExactlyInAnyOrder
                setOf(client1_2, client2_2)
    }


    @Test
    fun `Deletes client to subject mapping`() {
        val registry = TrieClientSubjectRegistry<String>()
        val clientSubscriptions = listOf(
            client1_1 to Subject.fromString("foo.*")
        )

        registry.addClients(clientSubscriptions)

        registry.remove(client1_1.clientKey, client1_1.subscriptionId)

        registry.clientsForSubject(Subject.fromString("foo.*")).shouldBeEmpty()
    }

    @Test
    fun `Deletes client to subject mapping when client has overlapping subjects`() {
        val registry = TrieClientSubjectRegistry<String>()
        val clientSubscriptions = listOf(
            client1_1 to Subject.fromString("foo.*"),
            client1_2 to Subject.fromString("foo.bar")
        )

        registry.addClients(clientSubscriptions)

        registry.remove(client1_1.clientKey, client1_1.subscriptionId)

        registry.clientsForSubject(Subject.fromString("foo.*")) shouldContainExactlyInAnyOrder setOf(
            client1_2
        )
    }

    private fun <KEY> TrieClientSubjectRegistry<KEY>.addClients(
        clientSubscriptions: List<Pair<ClientSubscriptionRef<KEY>, Subject>>
    ) {
        clientSubscriptions.forEach { (key, subject) ->
            this.add(key.clientKey, key.subscriptionId, subject)
        }
    }
}
