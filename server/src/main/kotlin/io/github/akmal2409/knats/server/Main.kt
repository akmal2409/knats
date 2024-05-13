package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.server.json.FlatJsonMarshaller
import io.github.akmal2409.knats.server.json.Lexer
import io.github.akmal2409.knats.server.parser.SuspendableParser
import io.github.akmal2409.knats.transport.NioTcpServerSocketChannelFactory
import io.github.akmal2409.knats.transport.SelectorFactoryImpl
import io.github.akmal2409.knats.transport.Transport
import java.net.InetSocketAddress
import kotlinx.coroutines.runBlocking


fun main() = runBlocking {
    val config = RealConfiguration()
    val jsonMarshaller = FlatJsonMarshaller(Lexer())


    val server = Transport(
        serverSocketChannelFactory = NioTcpServerSocketChannelFactory(
            InetSocketAddress(config.host, config.port)
        ),
        selectorFactory = SelectorFactoryImpl(),
        connectionHandler = ServiceConnectionHandler(config, TrieClientSubjectRegistry()),
        requestDeserializerFactory = {
            RequestDeserializer(
                SuspendableParser(
                    config.maxPayloadSize,
                    config.maxArgSize
                ), jsonMarshaller
            )
        },
        responseSerializer = { it }
    )

    server.start()

    Runtime.getRuntime().addShutdownHook(Thread(server::stop))
}
