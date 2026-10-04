# Kafka runtime compatibility

This contract runs the same compiled Cookie Cutter core and Kafka adapter under Node 24.18.0 and Bun 1.4.0. It supplements the existing Node/Jest suite without changing the framework's default runtime, dependencies, or build toolchain.

## What it checks

- The installed native `lz4-napi` codec compresses and decompresses a binary payload correctly.
- The real Kafka input adapter reads LZ4-compressed input records with the expected payload and exactly-once metadata.
- The real Kafka output adapter publishes LZ4-compressed records to a different topic in a transaction. Counters around the registered codec verify actual output compression and observer decompression calls.
- The transaction commits the next source offset (`3` after inputs `0`, `1`, `2`) for the input consumer group, with no group offset committed on the output topic. Assertions run before input release or disposal.
- A consumer using `read_committed` reads through a captured topic end, including transaction control records, then verifies the complete output payload list. A trailing duplicate must fail.
- All client cleanup operations succeed, followed by exit code zero without a forced exit. A Node supervisor rejects a child that prints success but leaves open handles until the deadline.

Each child uses fresh topic/group/transaction names. The child prints one validated JSON result only after assertions and cleanup finish. The supervisor preserves stdout, stderr, exit status, signal, and validation results, including on failure. Its default deadline is 120 seconds; `--timeout-ms` may override it up to 600,000 ms. A timed-out child is killed and the check fails.

[Kafka's transaction coordinator](https://github.com/apache/kafka/blob/4.3.1/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala#L623) can acknowledge `EndTxn` before applying its offset commit marker. The contract waits for the exact raw committed offset before releasing inputs or disposing the source. It does not use KafkaJS's `resolveOffsets` option, which could turn an unset offset into the topic's end and hide a missing commit. The original sink integration regression uses the same bounded observation principle.

The four supervisor regressions deliberately check a successful child, an open-handle hang after success output, a nonzero exit after success output, and accidental execution under the wrong runtime.

## Local validation — 2026-10-04

Both Node 24.18.0 and Bun 1.4.0 passed against Apache Kafka 4.3.1 on Linux x64. Each supervisor reported `validated: true`, `naturalExit: true`, and exit code zero. Both consumed inputs `0, 1, 2`, observed source offset `3` and destination offset `-1` before cleanup, invoked the output compressor and observer decompressor, and read through end offset `4` including the transaction marker. All cleanup operations succeeded.

The four supervisor tests and the updated Node/Jest sink integration regression passed. Two temporary negative-control copies were also checked under Node: disabling sink compression failed the codec-use assertion; appending a trailing duplicate produced `[0, 1, 2, 2]` and failed the complete-output assertion. Both failure cases completed cleanup. The temporary copies were removed.

Both runtimes emitted an existing KafkaJS 2.2.4 `TimeoutNegativeWarning` from its request queue's throttle timer. Assertions and process exit still passed; the supervisor preserves stderr rather than suppressing it.

## Run locally

Use Linux x64 with Docker Compose, Node 24.18.0, Yarn 1.22.22, and Bun 1.4.0. Port `30001` must be free. Run the following commands from the repository root, with the pinned Node executable on `PATH`:

```sh
yarn install --frozen-lockfile --ignore-scripts --non-interactive
yarn workspace @walmartlabs/cookie-cutter-core build
yarn workspace @walmartlabs/cookie-cutter-kafka build
node --test compatibility/run-kafka.test.cjs
docker compose -f compatibility/docker-compose.yaml up -d --wait --wait-timeout 180

node compatibility/run-kafka.cjs \
  --runtime="$(command -v node)" --expected-runtime=node \
  --broker=127.0.0.1:30001 --output=/tmp/cookie-cutter-compatibility/node.json
node compatibility/run-kafka.cjs \
  --runtime="$(command -v bun)" --expected-runtime=bun \
  --broker=127.0.0.1:30001 --output=/tmp/cookie-cutter-compatibility/bun.json

docker compose -f compatibility/docker-compose.yaml down --volumes --remove-orphans
```

Always run the final cleanup command, including when a check fails. Use the supervisor with **Node** for both child runtimes. The shared build and installed dependencies must remain unchanged between checks. Exit status zero and `validated: true` in each supervisor report are both required.

Installation skips workspace lifecycle scripts to avoid unrelated native builds and documentation install hooks. It retains optional dependencies: `lz4-napi` provides its Linux x64 native binary through an optional platform package, and the contract must actually invoke that binary's codec. This installation choice is specific to this contract and does not establish compatibility of every package in the monorepo.

The dedicated Apache Kafka 4.3.1 fixture has separate internal and host listeners, so its transaction coordinator can reach the broker without depending on the host's LAN address. It binds only to host loopback. Use it for local tests; this is not a production Kafka configuration. A custom `--broker` must point to an isolated test broker where creating and deleting test topics is appropriate.

## CI and scope

[Kafka Runtime Compatibility](../.github/workflows/runtime-compatibility.yml) installs dependencies and builds once, then runs Node and Bun sequentially against the same broker. A failed Node check remains a workflow failure while the Bun check still runs when setup succeeded. CI always removes the broker and uploads the JSON reports and logs as the `kafka-runtime-compatibility` artifact. The existing build/test workflow is unchanged.

This is a small compatibility contract, not a broker performance benchmark or a complete exactly-once guarantee. It does not cover crashes, rebalances, authentication/TLS, long-running recovery, all compression formats, or the other adapters. The output sink still cannot commit offsets for inputs absent from its output batch. See [the separate runtime benchmark](../benchmarks/RESULTS.md) for in-memory performance measurements.
