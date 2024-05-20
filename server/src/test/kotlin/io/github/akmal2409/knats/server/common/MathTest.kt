package io.github.akmal2409.knats.server.common

import io.kotest.matchers.ints.shouldBeExactly
import org.junit.jupiter.api.Test

class MathTest {

    @Test
    fun `Int#numberOfDigits works for negative, positive and 0`() {
        0.numberOfDigits() shouldBeExactly 1
        (-1).numberOfDigits() shouldBeExactly 1

        for (i in 2..9) {
            (-i).numberOfDigits() shouldBeExactly 1
            i.numberOfDigits() shouldBeExactly 1
        }

        for (i in 10..20) {
            (-i).numberOfDigits() shouldBeExactly 2
            i.numberOfDigits() shouldBeExactly 2
        }

        for (i in 1000..9999) {
            (-i).numberOfDigits() shouldBeExactly 4
            i.numberOfDigits() shouldBeExactly 4
        }
    }
}
