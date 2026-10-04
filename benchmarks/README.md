# Runtime baseline

`runtime.cjs` compares Node.js and Bun running the **same compiled Cookie Cutter core**. It has no dependencies beyond the repository installation and uses neither Bun APIs nor runtime-specific transpilation. Build once with the repository's Node/Yarn toolchain before either runtime executes the script:

```sh
yarn install --frozen-lockfile
yarn workspace @walmartlabs/cookie-cutter-core build
node benchmarks/runtime.cjs --scenario=startup
node benchmarks/runtime.cjs --scenario=stateless --count=100000
bun benchmarks/runtime.cjs --scenario=stateless --count=100000
node benchmarks/runtime.cjs --scenario=stateful --count=100000 --keys=128
bun benchmarks/runtime.cjs --scenario=stateful --count=100000 --keys=128
```

Use explicit executable paths to pin the runtime versions. Each invocation runs exactly one application in a fresh process and emits one JSON result. A failed correctness check exits unsuccessfully; only results with `validated: true` are usable.

## Workloads

| Scenario    | Work performed                                                                                                                                                                                                                                                   |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `startup`   | Load compiled core, build and run an empty finite application through initialization and shutdown. Requires `count=0`.                                                                                                                                           |
| `stateless` | Lazily generate input messages, dispatch each handler, publish one deterministic output, and release each input with its handler result.                                                                                                                         |
| `stateful`  | The same pipeline plus one cached state lookup and one persisted increment event per input, spread round-robin over the configured keys. Uses the actual `EventSourcedStateProvider`, `cached`, `InMemoryStateAggregationSource`, and `InMemoryStateOutputSink`. |

`--mode=concurrent` is the default and uses the framework's unchanged default concurrency, queue, batch, and yield settings. `--mode=serial` is an additional controlled baseline. Both use `LogAndFail`; errors must not become apparently fast successful samples. Logging, metrics, and tracing use the same default null implementations in both runtimes.

Options use `--name=value` syntax. Environment equivalents are `BENCH_SCENARIO`, `BENCH_MODE`, `BENCH_COUNT`, and `BENCH_KEYS`; command-line values take precedence. Defaults are `stateless`, `concurrent`, `100000` messages (`0` for startup), and `128` keys. Count is limited to 10 million and keys to 1 million so checksum arithmetic remains exact. Keys exceeding the message count are harmless; only visited keys are stored.

The source generates one message at a time instead of preallocating all inputs. Published outputs are folded into counters and checksums instead of being retained. Compact, preallocated ID bitsets verify every acknowledgement and output occurs exactly once, irrespective of completion order. Handler return values and output values are checked. Stateful validation additionally checks every stored event's ID, key, type and amount, final per-key event counts, and cached state totals and sequence numbers. It confirms those final reads hit the cache rather than reconstructing missing entries. The underlying in-memory source's load count is reported. This final state scan happens after the measured run. The actual in-memory event store intentionally retains events and is part of the stateful workload's memory cost.

## Measurement protocol

The dependency-free comparison driver automates the protocol for all three scenarios. Run it with Node, supplying explicit paths to both runtime executables:

```sh
node benchmarks/compare.cjs \
  --node=/absolute/path/to/node \
  --bun=/absolute/path/to/bun \
  --samples=5 --count=100000 --keys=128 \
  --output=/absolute/path/to/results.json
```

It runs one child process at a time, one rehearsal per runtime/scenario, then measured pairs with alternating starting order. `--mode=serial` selects the optional serial baseline. Each child has a five-minute timeout. The result records CPU/OS information, runtime version commands, repository HEAD and dirty status, arguments, execution order, all raw results/stdout/stderr, and median/minimum/maximum summaries. `wallMs` measures each entire child invocation, including process launch and exit. The output path is created as needed and overwritten after the run completes.

Failed subprocesses and invalid results are saved alongside successful attempts; they are never silently discarded. Summary statistics cover successful **measured** samples only and include explicit attempted/succeeded/failed counts. Rehearsals remain in the raw report but do not enter those statistics. Any failed attempt makes the driver's final `validated` value false and its exit status nonzero, after saving the report. Resolve failures before using a run for a runtime comparison. Stop unrelated benchmark, test, and broker activity before collecting performance results.

1. Record the repository commit, local diff, build command, exact runtime versions, CPU, OS, and benchmark arguments. Do not rebuild between runtimes or include installation/build time in runtime measurements.
2. Run each scenario once per runtime as an unreported rehearsal. This can warm filesystem caches; it does **not** warm a later process's JIT.
3. Execute at least five measured samples per runtime/scenario, one process at a time. Alternate the starting runtime between pairs (`Node, Bun`, then `Bun, Node`) to reduce ordering bias. Keep machine load, power settings, and arguments the same.
4. Save every JSON line and failed attempt. Compare medians and dispersion, not the fastest single run. Increase message count if runs are too brief, using the same count for both runtimes. Report startup separately.

`elapsedMs` measures `app.run()` from immediately before invocation through completion, including framework initialization, processing, draining, and disposal. It excludes module loading, application construction, and the final validation scan. Generator work, acknowledgement checking, and output checking are included. `messagesPerSecond` is input count divided by this time and is null for empty runs. `coreRequireMs`, `setupMs`, and `validationMs` are reported separately. `scriptElapsedMs` starts at the first script statement and ends before JSON serialization; it excludes executable launch/initialization and final process exit. Measure whole-process wall time externally when comparing cold startup, and identify that metric separately.

RSS fields are process resident-memory snapshots immediately before and after `app.run()`, in bytes. `rssDeltaBytes` is their difference; it is **not** peak memory, total allocation, or a controlled-GC heap measurement. No forced garbage collection or sampling timer is used. The after-run snapshot precedes the final state-validation scan. Report absolute RSS as well as any delta.

These are synthetic in-memory framework/runtime measurements. They include JavaScript allocation, promises, queues, scheduling, and validation overhead. They do not measure RabbitMQ, Kafka, Redis, network latency, codecs, production tracing, durable databases, real acknowledgements, failure recovery, or deployment startup. Throughput differences cannot be treated as end-to-end service speedups or adapter compatibility evidence.
