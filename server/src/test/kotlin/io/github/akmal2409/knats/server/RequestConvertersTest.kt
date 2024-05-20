package io.github.akmal2409.knats.server

import io.github.akmal2409.knats.extensions.remainingAsString
import io.github.akmal2409.knats.server.json.FlatJsonMarshaller
import io.github.akmal2409.knats.server.json.Lexer
import io.github.akmal2409.knats.server.parser.ConnectOperation
import io.github.akmal2409.knats.server.parser.PublishOperation
import io.github.akmal2409.knats.server.util.createArgsBuffer
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer
import kotlin.test.Test


class RequestConvertersTest {


    @Test
    fun `SubscribeRequest fromArgsBuffer maps when subId, id and queue group present`() {
        val argsBuffer = createArgsBuffer("subject", "group", "id")

        val request = SubscribeRequest.fromArgsBuffer(argsBuffer)

        request.subject shouldBe "subject"
        request.queueGroup shouldBe "group"
        request.subscriptionId shouldBe "id"
    }

    @Test
    fun `SubscribeRequest fromArgsBuffer maps when subId and id is present`() {
        val argsBuffer = createArgsBuffer("subject", "id")
        val request = SubscribeRequest.fromArgsBuffer(argsBuffer)

        request.subject shouldBe "subject"
        request.queueGroup.shouldBeNull()
        request.subscriptionId shouldBe "id"
    }

    @Test
    fun `convertToConnectRequest() converts with verbose flag and unknown flags`() {
        val connectOp = ConnectOperation(
            ByteBuffer.wrap(
                "{\"verbose\": true, \"unknown\": false}".toByteArray(Charsets.US_ASCII)
            )
        )

        val marshaller = FlatJsonMarshaller(Lexer())

        val request = convertToConnectRequest(connectOp, marshaller)

        request.verbose.shouldBeTrue()
    }


    @Test
    fun `MessageResponse toByteBuffer works when replyTo is not present`() {
        val expected = "MSG test.sub 123 4\r\ntest\r\n"
        val payload = "test"

        val msg = MessageResponse(
            "test.sub", "123", 4,
            ByteBuffer.wrap(payload.toByteArray(Charsets.US_ASCII))
        )

        msg.toByteBuffer().remainingAsString(Charsets.US_ASCII) shouldBe expected
    }

    @Test
    fun `MessageResponse toByteBuffer works when replyTo is present`() {
        val expected = "MSG test.sub 123 to.me 4\r\ntest\r\n"
        val payload = "test"

        val msg = MessageResponse(
            "test.sub", "123", 4,
            ByteBuffer.wrap(payload.toByteArray(Charsets.US_ASCII)),
            "to.me"
        )

        msg.toByteBuffer().remainingAsString(Charsets.US_ASCII) shouldBe expected
    }

    @Test
    fun `MessageResponse toByteBuffer works payload is empty`() {
        val expected = "MSG test.sub 123 0\r\n\r\n"

        val msg = MessageResponse(
            "test.sub", "123", 0,
            ByteBuffer.allocate(1)
        )

        val actual = msg.toByteBuffer().remainingAsString(Charsets.US_ASCII)
        actual shouldBe expected
    }

    @Test
    fun `PublishRequest fromOperation maps to domain when to reply to present`() {
        val args = "sub 4"
        val payload = "test"

        val operation = PublishOperation(
            ByteBuffer.wrap(args.toByteArray(Charsets.US_ASCII)),
            ByteBuffer.wrap(payload.toByteArray(Charsets.US_ASCII))
        )

        val actual = PublishRequest.fromOperation(operation)

        actual.subject shouldBe "sub"
        actual.payloadSize shouldBe 4
        actual.replyTo.shouldBeNull()
        actual.payload.remainingAsString(Charsets.US_ASCII) shouldBe payload
    }

    @Test
    fun `PublishRequest fromOperation maps to domain when reply to is present`() {
        val args = "sub reply.to 4"
        val payload = "test"

        val operation = PublishOperation(
            ByteBuffer.wrap(args.toByteArray(Charsets.US_ASCII)),
            ByteBuffer.wrap(payload.toByteArray(Charsets.US_ASCII))
        )

        val actual = PublishRequest.fromOperation(operation)

        actual.subject shouldBe "sub"
        actual.payloadSize shouldBe 4
        actual.replyTo shouldBe "reply.to"
        actual.payload.remainingAsString(Charsets.US_ASCII) shouldBe payload
    }

    @Test
    fun `PublishRequest fromOperation maps to domain payload is empty`() {
        val args = "sub 0"

        val operation = PublishOperation(
            ByteBuffer.wrap(args.toByteArray(Charsets.US_ASCII)),
            ByteBuffer.allocate(1)
        )

        val actual = PublishRequest.fromOperation(operation)

        actual.payloadSize shouldBe 0
    }

}
