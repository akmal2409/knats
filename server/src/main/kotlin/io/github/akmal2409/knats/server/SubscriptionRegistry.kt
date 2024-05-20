package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.transport.ClientKey
import java.util.concurrent.locks.ReentrantLock
import kotlinx.coroutines.channels.Channel
import kotlin.concurrent.withLock

/**
 * Provides insertion and query capabilities that can answer given a subject ->
 *  return all client keys associated with it
 * Given a subject (might be a wildcard) associate client with set of topics
 * Remove given client, subject all mappings
 */
interface ClientSubjectRegistry<KEY> {

    /**
     * Associates clientKey [KEY] with [subscriptionId] to a [subject]
     */
    fun add(clientKey: KEY, subscriptionId: String, subject: Subject)

    /**
     * Returns all [ClientSubscriptionRef] associated with a particular subject.
     */
    fun clientsForSubject(subject: Subject): Set<ClientSubscriptionRef<KEY>>

    /**
     * Removes association between a [clientKey] and a subject for that [subscriptionId]
     */
    fun remove(clientKey: KEY, subscriptionId: String)
}

data class ClientSubscriptionRef<KEY>(
    val clientKey: KEY,
    val subscriptionId: String
)

/**
 * Uses token-based trie based implementation to support wildcard tokens
 */
class TrieClientSubjectRegistry<KEY> : ClientSubjectRegistry<KEY> {

    private inner class TokenNode(
        val token: String,
        var childTokens: MutableMap<String, TokenNode>? = null,
        var clientKeys: MutableSet<ClientSubscriptionRef<KEY>>? = null
    ) {

        val nonWildcardChildren: List<TokenNode>?
            get() = childTokens?.values?.filter {
                it.token != Subject.SubjectToken.WILDCARD_TOKEN_PATTERN &&
                        it.token != Subject.SubjectToken.MATCH_REST_TOKEN_PATTERN
            }

        fun selectMatchRestClientKeys(block: (Set<ClientSubscriptionRef<KEY>>) -> Unit) =
            childTokens?.get(Subject.SubjectToken.MATCH_REST_TOKEN_PATTERN)
                ?.let { matchRestNode ->
                    matchRestNode.clientKeys?.let { block(it) }
                }
    }

    private val mutex = ReentrantLock()
    private val subscriptionToSubject = mutableMapOf<ClientSubscriptionRef<KEY>, Subject>()
    private val root = TokenNode("")

    override fun add(clientKey: KEY, subscriptionId: String, subject: Subject) {
        mutex.withLock {
            val clientSubscriptionRef = ClientSubscriptionRef(clientKey, subscriptionId)
            require(clientSubscriptionRef !in subscriptionToSubject) {
                "Subscription is already associated with a subject=${subscriptionToSubject[clientSubscriptionRef]}"
            }

            var nodeCursor = root

            for (token in subject.tokens) {
                val childTokens = nodeCursor.childTokens ?: HashMap()
                childTokens[token.value] = childTokens[token.value] ?: TokenNode(token.value)
                nodeCursor.childTokens = childTokens

                nodeCursor =
                    childTokens[token.value] ?: error("Internal error: $token was not present")
            }

            val clientKeys = nodeCursor.clientKeys ?: HashSet()
            clientKeys.add(clientSubscriptionRef)
            nodeCursor.clientKeys = clientKeys
            subscriptionToSubject[clientSubscriptionRef] = subject
        }
    }

    override fun clientsForSubject(subject: Subject): Set<ClientSubscriptionRef<KEY>> {
        mutex.withLock {
            val keys = HashSet<ClientSubscriptionRef<KEY>>()

            clientsForSubject(subject.tokens, root, keys)
            return keys
        }
    }

    override fun remove(clientKey: KEY, subscriptionId: String) {
        mutex.withLock {
            val clientSubscriptionRef = ClientSubscriptionRef(clientKey, subscriptionId)

            subscriptionToSubject[clientSubscriptionRef]?.let { subject ->
                removeForSubject(clientSubscriptionRef, subject)
            }

            subscriptionToSubject.remove(clientSubscriptionRef)
        }
    }

    private fun removeForSubject(clientSubscriptionRef: ClientSubscriptionRef<KEY>, subject: Subject) {
        var nodeCursor: TokenNode? = root
        var previous: TokenNode? = null

        for (token in subject.tokens) {
            previous = nodeCursor
            nodeCursor = nodeCursor?.childTokens?.get(token.value)

            if (nodeCursor == null) break
        }

        if (nodeCursor != null && previous != null) {
            nodeCursor.clientKeys?.remove(clientSubscriptionRef)

            if (nodeCursor.clientKeys?.isEmpty() == true) {
                nodeCursor.clientKeys = null
            }

            if (nodeCursor.childTokens?.isEmpty() == true) {
                previous.childTokens?.remove(subject.tokens.last().value)
            }
        }
    }

    @Suppress("LoopWithTooManyJumpStatements")
    private fun clientsForSubject(
        tokens: List<Subject.SubjectToken>,
        node: TokenNode,
        keys: MutableSet<ClientSubscriptionRef<KEY>>
    ) {
        if (tokens.isEmpty()) return

        var nodeCursor: TokenNode = node
        var nextNode: TokenNode? = null

        for ((index, token) in tokens.withIndex()) {
            when {
                token.isPlain() -> {
                    nextNode = nodeCursor.childTokens?.get(token.value)

                    // all clients that subscribed to token.>
                    nodeCursor.selectMatchRestClientKeys { keys.addAll(it) }

                    // all clients that subscribed to token.*.<token2>...
                    // if wildcard node exists, continue search recursively, by dropping one token
                    collectFromWildcardNode(nodeCursor, keys, tokens, index)
                }

                token.isMatchRest() -> {
                    require(index == tokens.lastIndex) { "No tokens after > expected" }
                    collectFromNodeTillEnd(nodeCursor, keys)
                    break
                }

                token.isWildcard() -> {
                    if (index == tokens.lastIndex) {
                        collectSingleLevelAtNode(nodeCursor, keys)
                    } else {
                        nodeCursor.nonWildcardChildren?.forEach { childNode ->
                            clientsForSubject(
                                tokens.subList(index + 1, tokens.size),
                                childNode,
                                keys
                            )
                        }
                    }
                    break
                }
            }

            if (nextNode == null) break
            nodeCursor = nextNode

            if (index == tokens.lastIndex) {
                nodeCursor.clientKeys?.let { nodeClientKeys -> keys.addAll(nodeClientKeys) }
            }
        }
    }

    /**
     * Collects
     */
    private fun collectFromWildcardNode(
        node: TokenNode, keys: MutableSet<ClientSubscriptionRef<KEY>>,
        tokens: List<Subject.SubjectToken>, tokenIndex: Int
    ) {
        node.childTokens?.get(Subject.SubjectToken.WILDCARD_TOKEN_PATTERN)
            ?.let { wildcardNode ->
                if (tokenIndex == tokens.lastIndex) {
                    wildcardNode.clientKeys?.let { keys.addAll(it) }
                } else {
                    clientsForSubject(
                        tokens.subList(tokenIndex + 1, tokens.size),
                        wildcardNode, keys
                    )
                }
            }
    }

    private fun collectFromNodeTillEnd(
        node: TokenNode,
        keys: MutableSet<ClientSubscriptionRef<KEY>>
    ) {
        collectSingleLevelAtNode(node, keys)

        node.childTokens?.values?.forEach { childNode ->
            collectFromNodeTillEnd(childNode, keys)
        }
    }

    private fun collectSingleLevelAtNode(
        node: TokenNode,
        keys: MutableSet<ClientSubscriptionRef<KEY>>
    ) {
        node.childTokens?.values?.forEach { childNode ->
            childNode.clientKeys?.let { childKeys -> keys.addAll(childKeys) }
        }
    }
}


data class SubscriptionKey(val subject: String, val subscriptionId: String)

interface SubscriptionRegistry {

    fun subscribe(clientKey: ClientKey, subscriptionKey: SubscriptionKey): Channel<Message>

    fun unsubscribe(clientKey: ClientKey, subscriptionKey: SubscriptionKey)
}

class InMemorySubscriptionRegistry : SubscriptionRegistry {


    override fun subscribe(
        clientKey: ClientKey,
        subscriptionKey: SubscriptionKey
    ): Channel<Message> {
        TODO("Not yet implemented")
    }

    override fun unsubscribe(clientKey: ClientKey, subscriptionKey: SubscriptionKey) {
        TODO("Not yet implemented")
    }
}

