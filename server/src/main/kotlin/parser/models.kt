package io.github.akmal2409.nats.server.parser

data class ConnectOptions(
    val verbose: Boolean = false,
    val pedantic: Boolean = false,
    val tlsRequired: Boolean = false
) {

    fun toJson(): String = """
        {"verbose": $verbose, "pedantic": $pedantic, "tlsRequired": $tlsRequired}
    """.trimIndent()
}
