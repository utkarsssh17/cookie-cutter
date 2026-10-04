# Node and Bun baseline — 2026-10-04

Bun ran the existing compiled core substantially faster in these local synthetic workloads. This supports evaluating Bun as an additional runtime before considering a framework rewrite. It does not establish production throughput or compatibility for every adapter.

## Results

Five measured samples per runtime and scenario, plus one excluded rehearsal per runtime and scenario. All 36 invocations passed the harness's correctness checks. Values below are medians with minimum–maximum ranges; lower elapsed time is better.

| Workload                                                 | Node 24.18.0                       | Bun 1.4.0                       | Ratio of Node/Bun median time |
| -------------------------------------------------------- | ---------------------------------- | ------------------------------- | ----------------------------- |
| Empty application, whole process                         | 71.50 ms (65.33–76.20)             | 59.66 ms (57.39–61.50)          | 1.20×                         |
| 100,000 stateless messages, application run              | 10,006.67 ms (9,890.31–10,129.92)  | 2,222.42 ms (2,178.08–2,247.69) | 4.50×                         |
| 100,000 stateful messages over 128 keys, application run | 11,117.43 ms (11,008.43–11,150.75) | 2,727.37 ms (2,653.99–2,761.33) | 4.08×                         |

Median throughput was 9,993 versus 44,996 messages/second for stateless processing, and 8,995 versus 36,665 messages/second for stateful processing. The stateful workload performs a cached lookup and persists one increment event per input. Every stateful invocation checked 100,000 stored events, 128 cached totals and sequence numbers, and exactly 128 source loads with no additional loads during final validation.

Median RSS snapshots immediately after the application run were 306.19 MiB (Node) versus 206.10 MiB (Bun) for stateless processing, and 344.68 MiB versus 242.07 MiB for stateful processing. These are resident-memory snapshots, **not peak memory**; runtimes can collect garbage at different times. The raw report also contains before-run snapshots and deltas.

The startup measurement is a fresh process after filesystem-cache rehearsals, including launch, module loading, an empty application's initialization/shutdown, and process exit. It is not cold-disk or deployment startup. Processing measurements include initialization, handler dispatch, queues, acknowledgement/output validation, draining, and disposal. Final state validation is excluded from those processing times. See [the measurement protocol](README.md#measurement-protocol) for the exact boundaries.

## Environment and reproduction

- Linux x64, kernel `7.0.0-38-generic`, Intel Core i9-12900K, 24 logical CPUs, approximately 61.3 GiB RAM.
- Node `24.18.0`, Bun `1.4.0`; Node 24 ran the comparison driver.
- Source commit `ed2db562b4e1acc5c10a3e3b1a1f4093618227d5`; working tree clean when measurement started.
- Same compiled CommonJS core and installed dependencies for both runtimes. No Bun-specific implementation or transpilation.
- Concurrent mode with the framework's default queue, batching, and yield settings; null logging, metrics, and tracing.
- Fresh sequential processes, alternating Node/Bun order between measured pairs. Test broker stopped and no concurrent test/build workloads from this task. This was a development machine, not an isolated or CPU-pinned benchmark host.
- Measurement window: `2026-10-04T16:25:48.722Z`–`2026-10-04T16:28:27.554Z`.

The installation used `yarn install --frozen-lockfile --ignore-scripts`. The core was built once with Node 24:

```sh
/path/to/node-24 node_modules/typescript/bin/tsc --project packages/core/tsconfig.json
/path/to/node-24 benchmarks/compare.cjs \
  --node=/path/to/node-24 --bun=/path/to/bun-1.4 \
  --samples=5 --count=100000 --keys=128 \
  --output=/absolute/path/to/results.json
```

[Raw samples and summaries](results/2026-10-04-node24-bun1.4.json) include exact executable paths, arguments, versions, host information, repository state, execution order, stdout/stderr, and validation outcomes. [Build provenance](results/2026-10-04-build.json) records the build/install commands, lockfile hash, and compiled core JavaScript hash. The compiled files were verified unchanged after measurement. Skipping install scripts was sufficient for this core-only workload; it is not a validated installation procedure for all native adapters.

## Reliability baseline and next step

This source includes the AMQP acknowledgement correction plus two further fixes: shutdown continues attempting top-level component disposal after an earlier failure; Kafka transactions commit the next source-topic offset after sending outputs, and transaction cleanup preserves the original failure if abort also fails.

Validation for the new fixes passed on Node 24: 224 core unit tests, 28 Kafka unit tests, and 14 Kafka integration tests against Apache Kafka 4.3.1. The broker regression verifies different input/output topics, the next committed input offset, no consumer-group offset on the output topic, and committed output visibility. It does not prove crash recovery, rebalance behavior, or atomicity under every failure. Inputs producing no output still cannot have their offsets committed by this sink's output-batch tracking. These Node results are not Bun adapter tests.

Next, add a small compiled-JavaScript Kafka contract to CI and run it on both runtimes, including transactional output/offset behavior, native LZ4 compression, and clean shutdown. Keep the existing Node test/build toolchain while evaluating runtime compatibility. Then measure a representative broker-backed service with its real codecs, tracing, and state store before choosing a default runtime. No Kafka, RabbitMQ, Redis, database, network, or production telemetry performance is measured here.
