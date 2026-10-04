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
import { Kafka, logLevel } from "kafkajs";
import * as os from "node:os";
import { KafkaMessagePublishingStrategy, KafkaMetadata, kafkaSink } from "..";
import { KafkaSink } from "../KafkaSink";

jest.setTimeout(60000);

function getHostIp(): string {
    for (const interfaces of Object.values(os.networkInterfaces())) {
        for (const address of interfaces ?? []) {
            if (address.family === "IPv4" && !address.internal) {
                return address.address;
            }
        }
    }
    return "127.0.0.1";
}

describe("Kafka exactly-once offset commits", () => {
    it("commits the next input offset while publishing to a different topic", async () => {
        const id = `${process.pid}-${Date.now()}`;
        const inputTopic = `eos-input-${id}`;
        const outputTopic = `eos-output-${id}`;
        const groupId = `eos-source-${id}`;
        const broker = `${process.env.HOST_IP || getHostIp()}:30001`;
        const client = new Kafka({
            clientId: `eos-test-${id}`,
            brokers: [broker],
            logLevel: logLevel.NOTHING,
        });
        const admin = client.admin();
        const producer = client.producer();
        const observer = client.consumer({
            groupId: `eos-observer-${id}`,
            readUncommitted: false,
        });
        const sink = kafkaSink({
            broker,
            defaultTopic: outputTopic,
            encoder: new JsonMessageEncoder(),
            messagePublishingStrategy: KafkaMessagePublishingStrategy.ExactlyOnceSemantics,
            transactionalId: `eos-transaction-${id}`,
        }) as KafkaSink;
        let readTimeout: ReturnType<typeof setTimeout> | undefined;

        try {
            await admin.connect();
            await admin.createTopics({
                waitForLeaders: true,
                topics: [inputTopic, outputTopic].map((topic) => ({
                    topic,
                    numPartitions: 1,
                    replicationFactor: 1,
                })),
            });
            await producer.connect();
            await producer.send({
                topic: inputTopic,
                messages: [0, 1, 2].map((value) => ({
                    key: "test-key",
                    value: JSON.stringify({ value }),
                })),
            });

            const outputs: IPublishedMessage[] = [0, 1, 2].map((value) => ({
                message: { type: "Processed", payload: { value } },
                metadata: { [KafkaMetadata.Key]: "test-key" },
                original: new MessageRef(
                    {
                        [KafkaMetadata.Topic]: inputTopic,
                        [KafkaMetadata.Partition]: 0,
                        [KafkaMetadata.Offset]: String(value),
                        [KafkaMetadata.ConsumerGroupId]: groupId,
                        [KafkaMetadata.ExactlyOnceSemantics]: true,
                    },
                    { type: "Input", payload: { value } }
                ),
                spanContext: null,
            }));
            await sink.initialize(DefaultComponentContext);
            await sink.sink(outputs.values());

            const offsets = await admin.fetchOffsets({
                groupId,
                topics: [inputTopic, outputTopic],
            });
            expect(offsets.find(({ topic }) => topic === inputTopic)?.partitions).toEqual([
                expect.objectContaining({ partition: 0, offset: "3" }),
            ]);
            expect(offsets.find(({ topic }) => topic === outputTopic)?.partitions).toEqual([
                expect.objectContaining({ partition: 0, offset: "-1" }),
            ]);

            await observer.connect();
            await observer.subscribe({ topic: outputTopic, fromBeginning: true });
            const received: number[] = [];
            let resolveReceived: () => void;
            const allReceived = new Promise<void>((resolve, reject) => {
                resolveReceived = resolve;
                readTimeout = setTimeout(
                    () => reject(new Error("Timed out reading committed output messages")),
                    30000
                );
            });
            await observer.run({
                eachMessage: async ({ message }) => {
                    received.push(JSON.parse(message.value.toString()).value);
                    if (received.length >= outputs.length) {
                        resolveReceived();
                    }
                },
            });
            await allReceived;
            expect(received).toEqual([0, 1, 2]);
        } finally {
            clearTimeout(readTimeout);
            await Promise.allSettled([
                observer.disconnect(),
                sink.dispose(),
                producer.disconnect(),
                admin.disconnect(),
            ]);
        }
    });
});
