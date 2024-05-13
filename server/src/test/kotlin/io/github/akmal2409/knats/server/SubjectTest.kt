package io.github.akmal2409.knats.server

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import java.lang.IllegalArgumentException
import kotlin.test.Test


class SubjectTest {

    @Test
    fun `Parses valid subjects correctly`() {
        val subjects = listOf(
            "*" to getExpectedSubjectFor("*"),
            "foo.*" to getExpectedSubjectFor("foo.*"),
            "foo.*.bar" to getExpectedSubjectFor("foo.*.bar"),
            "foo.>" to getExpectedSubjectFor("foo.>")
        )

        subjects.forEach { (subjectStr, expectedSubject) ->
            Subject.fromString(subjectStr) shouldBe expectedSubject
        }
    }

    @Test
    fun `Rejects invalid subjects`() {
        val subjects = listOf(">", "", ".", "foo.", ".bar",
            "foo.>.bar", "fo*.bar.*")

        subjects.forEach { subjectStr ->
            withClue("Expected $subjectStr to throw IllegalArgumentException") {
                shouldThrow<IllegalArgumentException> {
                    Subject.fromString(subjectStr)
                }
            }
        }
    }

    private fun getExpectedSubjectFor(subjectStr: String): Subject {
        val tokens = subjectStr.split(".")
        val mapped = mutableListOf<Subject.SubjectToken>()

        for (token in tokens) {
            if (token == Subject.SubjectToken.WILDCARD_TOKEN_PATTERN) {
                mapped.add(Subject.SubjectToken.WILDCARD_TOKEN)
            } else if (token == Subject.SubjectToken.MATCH_REST_TOKEN_PATTERN) {
                mapped.add(Subject.SubjectToken.MATCH_REST_TOKEN)
            } else {
                mapped.add(Subject.SubjectToken(token))
            }
        }

        return Subject(mapped)
    }
}
