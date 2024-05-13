package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.server.common.argumentError
import io.github.akmal2409.knats.transport.ClientKey
import java.nio.ByteBuffer
import java.time.Instant

/**
 * Represents serialized message payload that is formatted according
 * to the protocol. This is done to avoid serialization of the same message for each client.
 *
 * @param sender Client that sent the message
 * @param subject where the message is destined
 * @param serializedPayload formatted and serialized message according to MSG format
 * @param replyTo optional subject to reply to
 * @param receivedAt when the message was received
 */
data class Message(
    val sender: ClientKey,
    val subject: String,
    val serializedPayload: ByteBuffer,
    val replyTo: String?,
    val receivedAt: Instant
) {
}


data class Subject(val tokens: List<SubjectToken>) {

    data class SubjectToken(val value: String) {

        companion object {
            const val WILDCARD_TOKEN_PATTERN = "*"
            const val MATCH_REST_TOKEN_PATTERN = ">"

            val WILDCARD_TOKEN = SubjectToken(WILDCARD_TOKEN_PATTERN)
            val MATCH_REST_TOKEN = SubjectToken(MATCH_REST_TOKEN_PATTERN)

            private val PLAIN_TOKEN_PATTERN = Regex("[A-Za-z0-9]+")

            fun fromString(token: String) = when (token) {
                WILDCARD_TOKEN_PATTERN -> WILDCARD_TOKEN
                MATCH_REST_TOKEN_PATTERN -> MATCH_REST_TOKEN
                else -> SubjectToken(validate(token))
            }

            private fun validate(plainToken: String): String {
                require(PLAIN_TOKEN_PATTERN.matches(plainToken)) {
                    "$plainToken does not match ${PLAIN_TOKEN_PATTERN.pattern} pattern"
                }
                return plainToken
            }
        }

        fun isWildcard() = this === WILDCARD_TOKEN

        fun isMatchRest() = this === MATCH_REST_TOKEN

        fun isPlain() = !isWildcard() && !isMatchRest()
    }

    companion object {
        private const val TOKEN_SEPARATOR = '.'

        fun fromString(subject: String): Subject {
            val tokens = subject.split(TOKEN_SEPARATOR)
                .map(SubjectToken::fromString)

            require(tokens.isNotEmpty()) { "Invalid subject supplied $subject" }

            return Subject(validateTokens(tokens))
        }

        private fun validateTokens(tokens: List<SubjectToken>): List<SubjectToken> {

            for (i in 0..<tokens.size) {
                when {
                    i == 0 && tokens[i].isMatchRest() -> argumentError {
                        "> wildcard might only be used as " +
                                "last token (when there is a token before)"
                    }

                    tokens[i].isMatchRest() && i < tokens.size - 1 -> argumentError { "> wildcard should be last token" }
                }
            }

            return tokens
        }
    }
}

