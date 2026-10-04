/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

import {
    DefaultComponentContext,
    IPublishedMessage,
    JsonMessageEncoder,
    MessageRef,
} from "@walmartlabs/cookie-cutter-core";
import { Kafka } from "kafkajs";
import { DefaultKafkaHeaderNames, KafkaMessagePublishingStrategy, KafkaMetadata } from "..";
import { KafkaMessageProducer } from "../KafkaMessageProducer";
import { KafkaSink } from "../KafkaSink";

jest.mock("kafkajs", () => ({
    ...jest.requireActual("kafkajs"),
    Kafka: jest.fn(),
}));
jest.mock("../KafkaMessageProducer");

function inputMessage(
    topic = "input-events",
    offset = "42",
    partition = 0,
    consumerGroupId = "consumer-group",
    eos = true
): MessageRef {
    return new MessageRef(
        {
            [KafkaMetadata.Topic]: topic,
            [KafkaMetadata.Offset]: offset,
            [KafkaMetadata.Partition]: partition,
            [KafkaMetadata.ConsumerGroupId]: consumerGroupId,
            [KafkaMetadata.ExactlyOnceSemantics]: eos,
        },
        { type: "InputEvent", payload: {} }
    );
}

function publish(original: MessageRef, topic = "output-events"): IPublishedMessage {
    return {
        original,
        message: { type: "OutputEvent", payload: {} },
        metadata: { [KafkaMetadata.Topic]: topic, [KafkaMetadata.Key]: "key" },
        spanContext: original.spanContext,
    };
}

describe("KafkaSink transactions", () => {
    let sink: KafkaSink;
    let encoder: JsonMessageEncoder;
    let transaction: {
        sendOffsets: jest.Mock;
        commit: jest.Mock;
        abort: jest.Mock;
    };
    let producer: {
        connect: jest.Mock;
        disconnect: jest.Mock;
        transaction: jest.Mock;
    };
    let operations: string[];
    const sendMessages = jest.mocked(KafkaMessageProducer.prototype.sendMessages);

    async function initialize(
        strategy = KafkaMessagePublishingStrategy.ExactlyOnceSemantics
    ): Promise<void> {
        sink = new KafkaSink({
            broker: "localhost:9092",
            encoder,
            headerNames: DefaultKafkaHeaderNames,
            messagePublishingStrategy: strategy,
            transactionalId: "test-transaction",
        });
        await sink.initialize(DefaultComponentContext);
    }

    beforeEach(() => {
        operations = [];
        encoder = new JsonMessageEncoder();
        transaction = {
            sendOffsets: jest.fn(async () => {
                operations.push("offsets");
            }),
            commit: jest.fn(async () => {
                operations.push("commit");
            }),
            abort: jest.fn().mockResolvedValue(undefined),
        };
        producer = {
            connect: jest.fn().mockResolvedValue(undefined),
            disconnect: jest.fn().mockResolvedValue(undefined),
            transaction: jest.fn().mockResolvedValue(transaction),
        };
        jest.mocked(Kafka).mockImplementation(
            () => ({ producer: () => producer }) as unknown as Kafka
        );
        sendMessages.mockReset().mockImplementation(async (_messages, topic) => {
            operations.push(`send:${topic}`);
        });
    });

    afterEach(async () => {
        await sink?.dispose();
        jest.restoreAllMocks();
    });

    it("commits the next offset for the input topic after publishing to a different topic", async () => {
        await initialize();

        await sink.sink([publish(inputMessage())].values());

        expect(transaction.sendOffsets).toHaveBeenCalledTimes(1);
        expect(transaction.sendOffsets).toHaveBeenCalledWith({
            consumerGroupId: "consumer-group",
            topics: [{ topic: "input-events", partitions: [{ partition: 0, offset: "43" }] }],
        });
        expect(operations).toEqual(["send:output-events", "offsets", "commit"]);
        expect(transaction.abort).not.toHaveBeenCalled();
    });

    it("deduplicates fan-out and commits each source group and partition after all destinations", async () => {
        await initialize();
        const first = inputMessage("input-a", "4", 0, "group-a");

        await sink.sink(
            [
                publish(first, "output-x"),
                publish(first, "output-y"),
                publish(inputMessage("input-a", "9", 0, "group-a"), "output-y"),
                publish(inputMessage("input-a", "7", 0, "group-a"), "output-x"),
                publish(inputMessage("input-a", "2", 1, "group-a"), "output-y"),
                publish(inputMessage("input-a", "5", 0, "group-b"), "output-x"),
                publish(inputMessage("input-b", "0", 0, "group-a"), "output-y"),
            ].values()
        );

        expect(transaction.sendOffsets.mock.calls.map(([offsets]) => offsets)).toEqual([
            {
                consumerGroupId: "group-a",
                topics: [
                    {
                        topic: "input-a",
                        partitions: [
                            { partition: 0, offset: "10" },
                            { partition: 1, offset: "3" },
                        ],
                    },
                ],
            },
            {
                consumerGroupId: "group-b",
                topics: [{ topic: "input-a", partitions: [{ partition: 0, offset: "6" }] }],
            },
            {
                consumerGroupId: "group-a",
                topics: [{ topic: "input-b", partitions: [{ partition: 0, offset: "1" }] }],
            },
        ]);
        expect(operations).toEqual([
            "send:output-x",
            "send:output-y",
            "offsets",
            "offsets",
            "offsets",
            "commit",
        ]);
        expect(sendMessages.mock.calls.map(([messages]) => messages.length)).toEqual([3, 4]);
    });

    it("compares and increments offsets beyond Number.MAX_SAFE_INTEGER without precision loss", async () => {
        await initialize();

        await sink.sink(
            [
                publish(inputMessage("input-events", "9007199254740993")),
                publish(inputMessage("input-events", "9007199254740992")),
            ].values()
        );

        expect(transaction.sendOffsets).toHaveBeenCalledWith({
            consumerGroupId: "consumer-group",
            topics: [
                {
                    topic: "input-events",
                    partitions: [{ partition: 0, offset: "9007199254740994" }],
                },
            ],
        });
    });

    it("does not send consumer offsets in ordinary transactional mode", async () => {
        await initialize(KafkaMessagePublishingStrategy.Transactional);

        await sink.sink([publish(inputMessage())].values());

        expect(sendMessages).toHaveBeenCalledTimes(1);
        expect(transaction.sendOffsets).not.toHaveBeenCalled();
        expect(transaction.commit).toHaveBeenCalledTimes(1);
    });

    it("does not send offsets for input messages that do not request EoS", async () => {
        await initialize();

        await sink.sink(
            [publish(inputMessage("input-events", "42", 0, "consumer-group", false))].values()
        );

        expect(transaction.sendOffsets).not.toHaveBeenCalled();
        expect(transaction.commit).toHaveBeenCalledTimes(1);
    });

    it("does not substitute the destination topic when source topic metadata is absent", async () => {
        await initialize();
        const original = inputMessage();
        original.addMetadata({ [KafkaMetadata.Topic]: undefined });

        await sink.sink([publish(original)].values());

        expect(transaction.sendOffsets).not.toHaveBeenCalled();
        expect(transaction.commit).toHaveBeenCalledTimes(1);
    });

    it("publishes without starting a transaction in nontransactional mode", async () => {
        await initialize(KafkaMessagePublishingStrategy.NonTransactional);

        await sink.sink([publish(inputMessage())].values());

        expect(producer.transaction).not.toHaveBeenCalled();
        expect(sendMessages.mock.calls[0][2]).toBe(1);
        expect(sendMessages.mock.calls[0][3]).toBe(producer);
        expect(transaction.sendOffsets).not.toHaveBeenCalled();
    });

    it("aborts if sending offsets fails and preserves the error", async () => {
        await initialize();
        const error = new Error("offset commit failed");
        transaction.sendOffsets.mockRejectedValueOnce(error);

        await expect(sink.sink([publish(inputMessage())].values())).rejects.toBe(error);

        expect(transaction.abort).toHaveBeenCalledTimes(1);
        expect(transaction.commit).not.toHaveBeenCalled();
    });

    it("aborts if publishing fails before sending any offsets", async () => {
        await initialize();
        const error = new Error("publish failed");
        sendMessages.mockRejectedValueOnce(error);

        await expect(sink.sink([publish(inputMessage())].values())).rejects.toBe(error);

        expect(transaction.sendOffsets).not.toHaveBeenCalled();
        expect(transaction.abort).toHaveBeenCalledTimes(1);
        expect(transaction.commit).not.toHaveBeenCalled();
    });

    it("aborts if message encoding fails after starting the transaction", async () => {
        await initialize();
        const error = new Error("encode failed");
        jest.spyOn(encoder, "encode").mockImplementationOnce(() => {
            throw error;
        });

        await expect(sink.sink([publish(inputMessage())].values())).rejects.toBe(error);

        expect(sendMessages).not.toHaveBeenCalled();
        expect(transaction.abort).toHaveBeenCalledTimes(1);
        expect(transaction.commit).not.toHaveBeenCalled();
    });

    it("aborts if the transaction commit fails", async () => {
        await initialize();
        const error = new Error("transaction commit failed");
        transaction.commit.mockRejectedValueOnce(error);

        await expect(sink.sink([publish(inputMessage())].values())).rejects.toBe(error);

        expect(transaction.abort).toHaveBeenCalledTimes(1);
    });

    it.each(["publishing", "committing"])(
        "preserves the %s error when aborting also fails",
        async (phase) => {
            const logError = jest
                .spyOn(DefaultComponentContext.logger, "error")
                .mockImplementation(() => {});
            await initialize();
            const primaryError = new Error(`${phase} failed`);
            const abortError = new Error("abort failed");
            if (phase === "publishing") {
                sendMessages.mockRejectedValueOnce(primaryError);
            } else {
                transaction.commit.mockRejectedValueOnce(primaryError);
            }
            transaction.abort.mockRejectedValueOnce(abortError);

            await expect(sink.sink([publish(inputMessage())].values())).rejects.toBe(primaryError);

            expect(transaction.abort).toHaveBeenCalledTimes(1);
            expect(logError).toHaveBeenCalledWith("Failed to abort Kafka transaction", abortError);
        }
    );
});
