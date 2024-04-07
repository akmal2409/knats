package io.github.akmal2409.nats.transport

import java.net.InetSocketAddress
import java.net.ProtocolFamily
import java.net.StandardProtocolFamily
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel

fun interface SelectorFactory {

    fun create(): Selector
}

class SelectorFactoryImpl : SelectorFactory {
    override fun create() = Selector.open()
}

fun interface ServerSocketChannelFactory {

    fun create(): ServerSocketChannel
}

class NioTcpServerSocketChannelFactory(
    val bindAddress: InetSocketAddress = InetSocketAddress.createUnresolved("localhost", 0),
    val protocolFamily: ProtocolFamily = StandardProtocolFamily.INET
): ServerSocketChannelFactory {

    override fun create(): ServerSocketChannel = ServerSocketChannel.open(protocolFamily).apply {
        bind(bindAddress)
        configureBlocking(false)
    }
}
