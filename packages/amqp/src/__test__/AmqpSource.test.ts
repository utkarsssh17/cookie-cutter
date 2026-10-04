/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

import {
    DefaultComponentContext,
    JsonMessageEncoder,
    MessageRef,
} from "@walmartlabs/cookie-cutter-core";
import * as amqp from "amqplib";
import { AmqpSource } from "../AmqpSource";

jest.mock("amqplib", () => ({ connect: jest.fn() }));

describe("AmqpSource acknowledgements", () => {
    const message = {
        content: Buffer.from("{}"),
        fields: { redelivered: false },
        properties: { type: "TestMessage" },
    } as amqp.ConsumeMessage;

    let source: AmqpSource;
    let messages: AsyncIterableIterator<MessageRef>;
    let ack: jest.Mock;

    beforeEach(async () => {
        jest.useFakeTimers();
        ack = jest.fn();
        const channel = {
            ack,
            assertQueue: jest.fn().mockResolvedValue({}),
            prefetch: jest.fn(),
            checkQueue: jest.fn().mockResolvedValue({ messageCount: 0, consumerCount: 1 }),
            consume: jest.fn(
                async (
                    _queue: string,
                    onMessage: (message: amqp.ConsumeMessage) => Promise<void>
                ) => {
                    await onMessage(message);
                    return { consumerTag: "test-consumer" };
                }
            ),
        };
        const connection = {
            createChannel: jest.fn().mockResolvedValue(channel),
            close: jest.fn().mockResolvedValue(undefined),
        };
        jest.mocked(amqp.connect).mockResolvedValue(connection as unknown as amqp.ChannelModel);
        source = new AmqpSource({
            server: { host: "localhost" },
            queue: { name: "test-queue" },
            encoder: new JsonMessageEncoder(),
        });
        await source.initialize(DefaultComponentContext);
        messages = source.start();
    });

    afterEach(async () => {
        try {
            await source.stop();
            await messages.return();
            await source.dispose();
            expect(jest.getTimerCount()).toBe(0);
        } finally {
            jest.useRealTimers();
        }
    });

    it("does not acknowledge a message released with a processing error", async () => {
        const { value: messageRef } = await messages.next();

        await messageRef.release(undefined, new Error("handler failed"));

        expect(ack).not.toHaveBeenCalled();
    });

    it("acknowledges a successfully released message without a return value", async () => {
        const { value: messageRef } = await messages.next();

        await messageRef.release();

        expect(ack).toHaveBeenCalledTimes(1);
        expect(ack).toHaveBeenCalledWith(message);
    });

    it("acknowledges a successfully released message with a truthy return value", async () => {
        const { value: messageRef } = await messages.next();

        await messageRef.release({ ok: true });

        expect(ack).toHaveBeenCalledTimes(1);
        expect(ack).toHaveBeenCalledWith(message);
    });
});
