/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

// Keep KafkaJS informational logs off stdout, which carries the single result JSON.
// Errors remain visible on stderr. The supervisor also requires natural process exit.
process.env.KAFKAJS_LOG_LEVEL = "ERROR";

const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");

function runtimeDetails() {
    return {
        schemaVersion: 1,
        runtime: process.versions.bun ? "bun" : "node",
        runtimeVersion: process.versions.bun || process.versions.node,
    };
}

function brokerFrom(argv) {
    let broker = "127.0.0.1:30001";
    for (const arg of argv) {
        const match = /^--broker=(.+)$/.exec(arg);
        if (!match) throw new Error(`Unknown argument: ${arg}; expected --broker=host:port`);
        broker = match[1];
    }
    return broker;
}

async function bounded(label, operation, timeoutMs = 30000) {
    let timer;
    try {
        return await Promise.race([
            operation,
            new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error(`Timed out: ${label}`)), timeoutMs);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

function errorText(error) {
    return error?.stack || String(error);
}

async function main() {
    const broker = brokerFrom(process.argv.slice(2));
    const LZ4Codec = require("@2l/kafkajs-lz4");
    const { Kafka, CompressionCodecs, CompressionTypes, Partitioners } = require("kafkajs");
    const { DefaultComponentContext, JsonMessageEncoder } = require("../packages/core/dist");
    const {
        kafkaSource,
        kafkaSink,
        KafkaMetadata,
        KafkaMessagePublishingStrategy,
        KafkaPublisherCompressionMode,
    } = require("../packages/kafka/dist");
    const report = {
        ...runtimeDetails(),
        validated: false,
        broker,
        checks: {},
        frameworkWarnings: [],
        frameworkErrors: [],
        cleanup: { completed: [], errors: [] },
    };
    const context = {
        ...DefaultComponentContext,
        logger: {
            info() {},
            debug() {},
            warn(message) {
                report.frameworkWarnings.push(message);
            },
            error(message, error) {
                report.frameworkErrors.push({ message, error: errorText(error) });
            },
        },
    };
    const codec = new LZ4Codec().codec();
    const originalBuffer = Buffer.from("Cookie Cutter native LZ4 compatibility. ".repeat(64));
    const compressed = await bounded(
        "native LZ4 compression",
        codec.compress({ buffer: originalBuffer })
    );
    const decompressed = await bounded("native LZ4 decompression", codec.decompress(compressed));
    assert.ok(Buffer.isBuffer(compressed), "LZ4 compression must return a Buffer");
    assert.ok(Buffer.isBuffer(decompressed), "LZ4 decompression must return a Buffer");
    assert.deepEqual(decompressed, originalBuffer, "Native LZ4 roundtrip changed the payload");
    report.checks.nativeLz4 = {
        roundTrip: true,
        inputBytes: originalBuffer.length,
        compressedBytes: compressed.length,
        decompressedBytes: decompressed.length,
    };

    const id = randomUUID();
    const inputTopic = `compat-input-${id}`;
    const outputTopic = `compat-output-${id}`;
    const sourceGroupId = `compat-source-${id}`;
    const encoder = new JsonMessageEncoder();
    const client = new Kafka({
        clientId: `compat-${id}`,
        brokers: [broker],
        connectionTimeout: 5000,
        requestTimeout: 10000,
        retry: { retries: 3 },
    });
    const admin = client.admin();
    const producer = client.producer({ createPartitioner: Partitioners.LegacyPartitioner });
    const observer = client.consumer({
        groupId: `compat-observer-${id}`,
        readUncommitted: false,
        maxWaitTimeInMs: 100,
    });
    const source = kafkaSource({
        broker,
        encoder,
        topics: [inputTopic],
        group: sourceGroupId,
        eos: true,
    });
    const sink = kafkaSink({
        broker,
        encoder,
        defaultTopic: outputTopic,
        messagePublishingStrategy: KafkaMessagePublishingStrategy.ExactlyOnceSemantics,
        compressionMode: KafkaPublisherCompressionMode.LZ4,
        transactionalId: `compat-transaction-${id}`,
    });
    let iterator;
    let adminConnected = false;
    let topicsCreated = false;
    let failure;
    let originalLz4Factory;
    let codecPhase;
    const codecCalls = { sinkCompression: 0, observerDecompression: 0 };
    const refs = [];

    async function cleanup(label, action) {
        try {
            await bounded(label, action(), 15000);
            report.cleanup.completed.push(label);
        } catch (error) {
            report.cleanup.errors.push({ step: label, error: errorText(error) });
        }
    }

    try {
        await bounded("admin connect", admin.connect());
        adminConnected = true;
        await bounded(
            "create topics",
            admin.createTopics({
                waitForLeaders: true,
                topics: [inputTopic, outputTopic].map((topic) => ({
                    topic,
                    numPartitions: 1,
                    replicationFactor: 1,
                })),
            })
        );
        topicsCreated = true;
        await bounded("input producer connect", producer.connect());
        await bounded(
            "publish compressed inputs",
            producer.send({
                topic: inputTopic,
                compression: CompressionTypes.LZ4,
                messages: [0, 1, 2].map((value) => ({
                    key: "test-key",
                    value: JSON.stringify({ value }),
                    headers: { event_type: "Input" },
                })),
            })
        );
        await source.initialize(context);
        iterator = source.start({ evict: async () => {} });
        for (let value = 0; value < 3; value++) {
            const next = await bounded(`read input ${value}`, iterator.next());
            assert.equal(next.done, false, "KafkaSource ended before receiving three inputs");
            const ref = next.value;
            assert.equal(ref.metadata(KafkaMetadata.Topic), inputTopic);
            assert.equal(ref.metadata(KafkaMetadata.Partition), 0);
            assert.equal(ref.metadata(KafkaMetadata.Offset), String(value));
            assert.equal(ref.metadata(KafkaMetadata.ConsumerGroupId), sourceGroupId);
            assert.equal(ref.metadata(KafkaMetadata.ExactlyOnceSemantics), true);
            assert.equal(ref.payload.type, "Input");
            assert.deepEqual(ref.payload.payload, { value });
            refs.push(ref);
        }
        report.checks.source = {
            compressedInput: "LZ4",
            eos: true,
            received: refs.length,
            values: refs.map((ref) => ref.payload.payload.value),
            offsets: refs.map((ref) => ref.metadata(KafkaMetadata.Offset)),
        };
        await bounded("sink initialize", sink.initialize(context));
        // Wrap the registered native codec after initialization, without replacing its work.
        // Scope counters so the initial native roundtrip and input traffic cannot satisfy them.
        originalLz4Factory = CompressionCodecs[CompressionTypes.LZ4];
        CompressionCodecs[CompressionTypes.LZ4] = (...args) => {
            const registeredCodec = originalLz4Factory(...args);
            return {
                async compress(...parameters) {
                    const result = await registeredCodec.compress(...parameters);
                    if (codecPhase === "sink") codecCalls.sinkCompression++;
                    return result;
                },
                async decompress(...parameters) {
                    const result = await registeredCodec.decompress(...parameters);
                    if (codecPhase === "observer") codecCalls.observerDecompression++;
                    return result;
                },
            };
        };
        const outputs = refs.map((original) => ({
            original,
            message: { type: "Processed", payload: original.payload.payload },
            metadata: { [KafkaMetadata.Key]: "test-key" },
            spanContext: original.spanContext,
        }));
        codecPhase = "sink";
        await bounded("transactional compressed output", sink.sink(outputs.values()));
        codecPhase = undefined;
        assert.ok(
            codecCalls.sinkCompression > 0,
            "KafkaSink did not use the registered LZ4 compressor"
        );

        // Verify before release or disposal, so cleanup cannot supply missing commits.
        // Kafka can acknowledge EndTxn before writing transaction markers. Poll raw
        // offsets until visible; resolveOffsets:true would turn unset -1 into the high
        // watermark and could falsely pass without any committed consumer offset.
        const offsetDeadline = Date.now() + 30000;
        let offsetAttempts = 0;
        let inputPartitions;
        let outputPartitions;
        while (true) {
            const offsets = await bounded(
                "read committed group offsets",
                admin.fetchOffsets({
                    groupId: sourceGroupId,
                    topics: [inputTopic, outputTopic],
                    resolveOffsets: false,
                }),
                Math.max(1, offsetDeadline - Date.now())
            );
            offsetAttempts++;
            inputPartitions = offsets.find(({ topic }) => topic === inputTopic)?.partitions;
            outputPartitions = offsets.find(({ topic }) => topic === outputTopic)?.partitions;
            assert.equal(inputPartitions?.length, 1);
            assert.equal(inputPartitions[0].partition, 0);
            assert.equal(outputPartitions?.length, 1);
            assert.equal(outputPartitions[0].partition, 0);
            assert.equal(
                outputPartitions[0].offset,
                "-1",
                "Destination group offset must remain unset"
            );
            if (inputPartitions[0].offset === "3") break;
            assert.ok(
                Date.now() < offsetDeadline,
                `Timed out waiting for source offset 3; received ${inputPartitions[0].offset}`
            );
            await new Promise((resolve) =>
                setTimeout(resolve, Math.min(100, offsetDeadline - Date.now()))
            );
        }
        report.checks.transaction = {
            compression: "LZ4",
            sourceNextOffset: inputPartitions[0].offset,
            destinationGroupOffset: outputPartitions[0].offset,
            checkedBeforeReleaseAndDispose: true,
            offsetAttempts,
            sinkCompressionCalls: codecCalls.sinkCompression,
        };
        for (const ref of refs) await ref.release();

        const outputOffsets = await bounded(
            "snapshot output end",
            admin.fetchTopicOffsets(outputTopic)
        );
        assert.equal(outputOffsets.length, 1);
        assert.equal(outputOffsets[0].partition, 0);
        const outputEndOffset = BigInt(outputOffsets[0].high);
        assert.ok(outputEndOffset > 0n, "Output topic must contain records");
        const received = [];
        let complete;
        let observerError;
        let observedEndOffset;
        const reachedOutputEnd = new Promise((resolve) => {
            complete = resolve;
        });
        observer.on(observer.events.CRASH, ({ payload }) => {
            observerError = payload.error;
            complete();
        });
        observer.on(observer.events.END_BATCH_PROCESS, ({ payload }) => {
            if (payload.topic === outputTopic && payload.partition === 0) {
                // KafkaJS includes transaction control records in lastOffset, including
                // control-only batches. Drain to the frozen end, not merely three values.
                const nextOffset = BigInt(payload.lastOffset) + 1n;
                if (nextOffset >= outputEndOffset) {
                    observedEndOffset = nextOffset.toString();
                    complete();
                }
            }
        });
        await bounded("observer connect", observer.connect());
        codecPhase = "observer";
        await bounded(
            "observer subscribe",
            observer.subscribe({ topic: outputTopic, fromBeginning: true })
        );
        await bounded(
            "observer start",
            observer.run({
                autoCommit: false,
                eachMessage: async ({ message }) => {
                    try {
                        assert.equal(message.headers.event_type.toString(), "Processed");
                        received.push(JSON.parse(message.value.toString()).value);
                    } catch (error) {
                        observerError = error;
                    }
                    if (observerError) complete();
                },
            })
        );
        await bounded("read committed outputs through frozen end", reachedOutputEnd);
        await bounded("observer stop", observer.stop());
        codecPhase = undefined;
        if (observerError) throw observerError;
        assert.deepEqual(received, [0, 1, 2]);
        assert.ok(
            codecCalls.observerDecompression > 0,
            "Observer did not use the registered LZ4 decompressor"
        );
        report.checks.observer = {
            readCommitted: true,
            received: received.length,
            values: received,
            outputEndOffset: outputEndOffset.toString(),
            observedEndOffset,
            decompressionCalls: codecCalls.observerDecompression,
        };
    } catch (error) {
        failure = error;
    } finally {
        // Closing the source pipe first releases any pending next()/send() before disconnect.
        await cleanup("source.stop", () => source.stop());
        if (iterator) await cleanup("source.iterator.return", () => iterator.return());
        await Promise.all([
            cleanup("source.dispose", () => source.dispose()),
            cleanup("sink.dispose", () => sink.dispose()),
            cleanup("observer.disconnect", () => observer.disconnect()),
            cleanup("producer.disconnect", () => producer.disconnect()),
        ]);
        if (adminConnected && topicsCreated) {
            await cleanup("admin.deleteTopics", () =>
                admin.deleteTopics({ topics: [inputTopic, outputTopic] })
            );
        }
        await cleanup("admin.disconnect", () => admin.disconnect());
        if (originalLz4Factory) CompressionCodecs[CompressionTypes.LZ4] = originalLz4Factory;
    }
    if (failure) report.error = errorText(failure);
    report.validated =
        !failure && report.cleanup.errors.length === 0 && report.frameworkErrors.length === 0;
    return report;
}

main()
    .then((report) => {
        if (report.validated) {
            console.log(JSON.stringify(report));
        } else {
            console.error(JSON.stringify(report));
            process.exitCode = 1;
        }
    })
    .catch((error) => {
        console.error(
            JSON.stringify({ ...runtimeDetails(), validated: false, error: errorText(error) })
        );
        process.exitCode = 1;
    });
