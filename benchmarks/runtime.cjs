/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

// Run the same compiled framework and this same script in each runtime.
const scriptStarted = process.hrtime.bigint();
const elapsedMs = (since) => Number(process.hrtime.bigint() - since) / 1e6;

function optionsFrom(argv) {
    const options = {
        scenario: process.env.BENCH_SCENARIO || "stateless",
        mode: process.env.BENCH_MODE || "concurrent",
        count: process.env.BENCH_COUNT,
        keys: process.env.BENCH_KEYS || "128",
    };
    for (const arg of argv) {
        const match = /^--(scenario|mode|count|keys)=(.+)$/.exec(arg);
        if (!match) {
            throw new Error(`Unknown argument: ${arg}. Use --help for usage.`);
        }
        options[match[1]] = match[2];
    }
    if (!["startup", "stateless", "stateful"].includes(options.scenario)) {
        throw new Error("scenario must be startup, stateless, or stateful");
    }
    if (!["serial", "concurrent"].includes(options.mode)) {
        throw new Error("mode must be serial or concurrent");
    }
    options.count = Number(options.count ?? (options.scenario === "startup" ? 0 : 100000));
    options.keys = Number(options.keys);
    if (!Number.isSafeInteger(options.count) || options.count < 0 || options.count > 10000000) {
        throw new Error("count must be an integer between 0 and 10000000");
    }
    if (!Number.isSafeInteger(options.keys) || options.keys < 1 || options.keys > 1000000) {
        throw new Error("keys must be an integer between 1 and 1000000");
    }
    if (options.scenario === "startup" && options.count !== 0) {
        throw new Error("startup requires count=0");
    }
    return options;
}

function check(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}

// Validate every ID exactly once without retaining message objects.
function idCounter(size) {
    const seen = new Uint8Array(Math.ceil(size / 8));
    return {
        count: 0,
        checksum: 0,
        add(id) {
            if (!Number.isInteger(id) || id <= 0 || id > size) {
                throw new Error(`Invalid ID: ${id}`);
            }
            const index = id - 1;
            const mask = 1 << (index & 7);
            if ((seen[index >> 3] & mask) !== 0) {
                throw new Error(`Duplicate ID: ${id}`);
            }
            seen[index >> 3] |= mask;
            this.count++;
            this.checksum += id;
        },
        verify() {
            check(this.count === size, `Expected ${size} IDs, received ${this.count}`);
            check(this.checksum === (size * (size + 1)) / 2, "ID checksum mismatch");
            return { count: this.count, checksum: this.checksum };
        },
    };
}

async function main() {
    const options = optionsFrom(process.argv.slice(2));
    const requireStarted = process.hrtime.bigint();
    const {
        Application,
        cached,
        ErrorHandlingMode,
        EventSourcedStateProvider,
        InMemoryStateAggregationSource,
        InMemoryStateOutputSink,
        MessageRef,
        OutputSinkConsistencyLevel,
        ParallelismMode,
        StaticInputSource,
    } = require("../packages/core/dist");
    const coreRequireMs = elapsedMs(requireStarted);
    const coreVersion = require("../packages/core/package.json").version;
    const setupStarted = process.hrtime.bigint();
    const stateful = options.scenario === "stateful";
    const keys = Array.from(
        { length: Math.min(options.keys, options.count) },
        (_, i) => `key-${i}`
    );
    const acknowledgements = idCounter(options.count);
    const published = idCounter(options.count);
    let generated = 0;
    let publishedValueChecksum = 0;
    let releaseErrors = 0;

    async function released(ref, value, error) {
        if (error) {
            releaseErrors++;
            throw error;
        }
        check(value === ref.payload.payload.id, "Handler response mismatch");
        acknowledgements.add(value);
    }

    function* inputs() {
        for (let id = 1; id <= options.count; id++) {
            const ref = new MessageRef(
                {},
                {
                    type: "Work",
                    payload: { id, key: keys[(id - 1) % options.keys] },
                }
            );
            ref.once("released", released);
            generated++;
            yield ref;
        }
    }

    class Output {}
    class Added {}
    class CounterState {
        constructor(snapshot) {
            this.total = snapshot ? snapshot.total : 0;
        }
        snap() {
            return { total: this.total };
        }
    }
    class CountingStateSource extends InMemoryStateAggregationSource {
        constructor(storage) {
            super(storage);
            this.loads = 0;
        }
        load(...args) {
            this.loads++;
            return super.load(...args);
        }
    }
    const storage = new Map();
    const stateSource = new CountingStateSource(storage);
    const stateProvider = stateful
        ? cached(
              CounterState,
              new EventSourcedStateProvider(
                  CounterState,
                  {
                      onAdded(event, state) {
                          state.total += event.amount;
                      },
                  },
                  stateSource
              ),
              { maxSize: options.keys }
          )
        : undefined;
    const sink = {
        guarantees: { consistency: OutputSinkConsistencyLevel.Atomic, idempotent: false },
        async sink(messages) {
            for (const item of messages) {
                const { id, value } = item.message.payload;
                check(item.message.type === "Output", "Unexpected output type");
                check(value === id * 2, "Output value mismatch");
                published.add(id);
                publishedValueChecksum += value;
            }
        },
    };
    const handler = stateful
        ? {
              async onWork(message, context) {
                  const state = await context.state.get(message.key);
                  context.store(Added, state, { id: message.id, amount: 1 });
                  context.publish(Output, { id: message.id, value: message.id * 2 });
                  return message.id;
              },
          }
        : {
              onWork(message, context) {
                  context.publish(Output, { id: message.id, value: message.id * 2 });
                  return message.id;
              },
          };
    const app = Application.create()
        .input()
        .add(new StaticInputSource(inputs()))
        .done()
        .dispatch(handler)
        .output()
        .published(sink)
        .done();
    if (stateful) {
        app.state(stateProvider).output().stored(new InMemoryStateOutputSink(storage)).done();
    }
    const setupMs = elapsedMs(setupStarted);
    const rssBeforeBytes = process.memoryUsage().rss;
    const runStarted = process.hrtime.bigint();
    await app.run(
        ErrorHandlingMode.LogAndFail,
        options.mode === "serial" ? ParallelismMode.Serial : ParallelismMode.Concurrent
    );
    const elapsed = elapsedMs(runStarted);
    const rssAfterBytes = process.memoryUsage().rss;

    const validationStarted = process.hrtime.bigint();
    check(generated === options.count, "Generated input count mismatch");
    check(releaseErrors === 0, "Some messages failed processing");
    const ackResult = acknowledgements.verify();
    const publishResult = published.verify();
    check(
        publishedValueChecksum === options.count * (options.count + 1),
        "Output checksum mismatch"
    );
    const stateResult = {
        keys: 0,
        events: 0,
        eventIdChecksum: 0,
        total: 0,
        weightedTotal: 0,
        sourceLoads: stateSource.loads,
    };
    if (stateful) {
        const stored = idCounter(options.count);
        check(storage.size === keys.length, "Stored key count mismatch");
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const events = storage.get(key);
            const expected = Math.floor((options.count - 1 - i) / options.keys) + 1;
            check(events.length === expected, `Stored event count mismatch for ${key}`);
            for (const event of events) {
                const { id, amount } = event.payload;
                check(event.type === "Added" && amount === 1, "Stored event content mismatch");
                check((id - 1) % options.keys === i, "Event stored under incorrect key");
                stored.add(id);
            }
            // This provider has no external resources; inspect its retained cache after disposal.
            const cachedState = await stateProvider.get(null, key);
            check(cachedState.seqNum === expected, `Cached sequence mismatch for ${key}`);
            check(cachedState.state.total === expected, `Cached state mismatch for ${key}`);
            stateResult.keys++;
            stateResult.total += cachedState.state.total;
            stateResult.weightedTotal += (i + 1) * cachedState.state.total;
        }
        const storedResult = stored.verify();
        stateResult.events = storedResult.count;
        stateResult.eventIdChecksum = storedResult.checksum;
        check(stateResult.total === options.count, "Final state total mismatch");
        check(
            stateSource.loads === stateResult.sourceLoads,
            "Final state validation missed the cache"
        );
    }
    const validationMs = elapsedMs(validationStarted);
    console.log(
        JSON.stringify({
            schemaVersion: 1,
            runtime: process.versions.bun ? "bun" : "node",
            runtimeVersion: process.versions.bun || process.versions.node,
            nodeCompatibilityVersion: process.versions.node,
            platform: process.platform,
            arch: process.arch,
            coreVersion,
            ...options,
            coreRequireMs,
            setupMs,
            elapsedMs: elapsed,
            messagesPerSecond: options.count === 0 ? null : options.count / (elapsed / 1000),
            validationMs,
            scriptElapsedMs: elapsedMs(scriptStarted),
            rssBeforeBytes,
            rssAfterBytes,
            rssDeltaBytes: rssAfterBytes - rssBeforeBytes,
            validated: true,
            generated,
            acknowledgements: ackResult,
            published: { ...publishResult, valueChecksum: publishedValueChecksum },
            releaseErrors,
            state: stateResult,
        })
    );
}

if (process.argv.slice(2).includes("--help")) {
    console.log(
        "Usage: <node|bun> benchmarks/runtime.cjs [--scenario=startup|stateless|stateful] [--mode=serial|concurrent] [--count=100000] [--keys=128]"
    );
} else {
    main().catch((error) => {
        console.error(JSON.stringify({ validated: false, error: error.stack || String(error) }));
        process.exitCode = 1;
    });
}
