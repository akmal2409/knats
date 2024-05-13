package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.transport.ClientKey
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import kotlin.test.Test


class TrieClientSubjectRegistryTest {

    val client1 = ClientKey.fromRemoteAddress("client-1")
    val client2 = ClientKey.fromRemoteAddress("client-2")
    val client3 = ClientKey.fromRemoteAddress("client-3")
    val client4 = ClientKey.fromRemoteAddress("client-4")

    @Test
    fun `Supports plain token queries`() {
        val registry = TrieClientSubjectRegistry<ClientKey>()
        val clientSubscriptions = listOf(
            client1 to listOf(
                Subject.fromString("foo")
            ),
            client2 to listOf(
                Subject.fromString("fooz")
            ),
            client3 to listOf(
                Subject.fromString("foo.bar")
            ),
            client4 to listOf(
                Subject.fromString("foo.bar.star")
            )
        )

        registry.addClients(clientSubscriptions)

        registry.clientsForSubject(Subject.fromString("foo")) shouldContainExactlyInAnyOrder setOf(
            client1
        )

        registry.clientsForSubject(Subject.fromString("fooz")) shouldContainExactlyInAnyOrder setOf(
            client2
        )

        registry.clientsForSubject(Subject.fromString("foo.bar")) shouldContainExactlyInAnyOrder setOf(
            client3
        )

        registry.clientsForSubject(Subject.fromString("foo.bar.star")) shouldContainExactlyInAnyOrder setOf(
            client4
        )
    }

    @Test
    fun `Supports wildcard query`() {
        val registry = TrieClientSubjectRegistry<ClientKey>()
        val clientSubscriptions = listOf(
            client1 to listOf(
                Subject.fromString("foo.*")
            ),
            client2 to listOf(
                Subject.fromString("foo.bar")
            ),
            client3 to listOf(
                Subject.fromString("foo.bar.star"), Subject.fromString("bar.star")
            ),
            client4 to listOf(
                Subject.fromString("foo.lol.star")
            )
        )

        registry.addClients(clientSubscriptions)

        registry.clientsForSubject(Subject.fromString("foo.*")) shouldContainExactlyInAnyOrder
                setOf(client1, client2)

        registry.clientsForSubject(Subject.fromString("foo.*.star")) shouldContainExactlyInAnyOrder
                setOf(client3, client4)
    }

    @Test
    fun `Supports match rest query`() {
        val registry = TrieClientSubjectRegistry<ClientKey>()
        val clientSubscriptions = listOf(
            client1 to listOf(
                Subject.fromString("foo.*"), Subject.fromString("first.*.second.*.third")
            ),
            client2 to listOf(
                Subject.fromString("foo.bar.star.lol"), Subject.fromString("first.third.*")
            ),
            client3 to listOf(
                Subject.fromString("bar")
            ),
            client4 to listOf(
                Subject.fromString("foo.me.star")
            )
        )

        registry.addClients(clientSubscriptions)

        registry.clientsForSubject(Subject.fromString("foo.>")) shouldContainExactlyInAnyOrder
                setOf(client1, client2, client4)

        registry.clientsForSubject(Subject.fromString("first.>")) shouldContainExactlyInAnyOrder
                setOf(client1, client2)
    }

    private fun <KEY> TrieClientSubjectRegistry<KEY>.addClients(clientSubscriptions: List<Pair<KEY, List<Subject>>>) {
        clientSubscriptions.forEach { (key, subjects) ->
            subjects.forEach { subject -> this.add(key, subject) }
        }
    }
}
