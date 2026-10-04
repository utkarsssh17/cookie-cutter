/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

// Keep this supervisor on Node so the same watchdog checks either child runtime.
const { spawnSync } = require("node:child_process");
const { mkdirSync, writeFileSync } = require("node:fs");
const path = require("node:path");

const usage =
    "Usage: node compatibility/run-kafka.cjs --runtime=/path/to/node-or-bun --expected-runtime=node|bun --broker=127.0.0.1:30001 --output=/path/result.json [--timeout-ms=120000]";

function parseOptions(argv) {
    const options = { "timeout-ms": 120000 };
    for (const arg of argv) {
        const match = /^--(runtime|expected-runtime|broker|output|timeout-ms)=(.+)$/.exec(arg);
        if (!match) throw new Error(`Unknown argument: ${arg}\n${usage}`);
        options[match[1]] = match[2];
    }
    for (const name of ["runtime", "expected-runtime", "broker", "output"]) {
        if (!options[name]) throw new Error(`Missing --${name}\n${usage}`);
    }
    if (!["node", "bun"].includes(options["expected-runtime"])) {
        throw new Error("--expected-runtime must be node or bun");
    }
    const timeoutMs = Number(options["timeout-ms"]);
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 600000) {
        throw new Error("--timeout-ms must be an integer between 1 and 600000");
    }
    return {
        runtime: path.resolve(options.runtime),
        expectedRuntime: options["expected-runtime"],
        broker: options.broker,
        output: path.resolve(options.output),
        timeoutMs,
    };
}

function runContract(options, script = path.join(__dirname, "kafka.cjs")) {
    const args = [script, `--broker=${options.broker}`];
    const startedAt = new Date().toISOString();
    const started = process.hrtime.bigint();
    const child = spawnSync(options.runtime, args, {
        cwd: path.resolve(__dirname, ".."),
        encoding: "utf8",
        timeout: options.timeoutMs,
        // A stalled child must not be able to ignore the deadline.
        killSignal: "SIGKILL",
        maxBuffer: 1024 * 1024,
    });
    const durationMs = Number(process.hrtime.bigint() - started) / 1e6;
    let result = null;
    let validationError = null;
    try {
        result = JSON.parse(child.stdout || "");
        if (
            result?.schemaVersion !== 1 ||
            result.validated !== true ||
            result.runtime !== options.expectedRuntime ||
            typeof result.runtimeVersion !== "string" ||
            result.runtimeVersion.length === 0
        ) {
            throw new Error("Child did not return a validated result for the expected runtime");
        }
    } catch (error) {
        validationError = error.message;
    }
    const naturalExit = !child.error && child.status === 0 && child.signal === null;
    return {
        schemaVersion: 1,
        startedAt,
        finishedAt: new Date().toISOString(),
        command: [options.runtime, ...args],
        expectedRuntime: options.expectedRuntime,
        timeoutMs: options.timeoutMs,
        durationMs,
        validated: naturalExit && validationError === null,
        naturalExit,
        exitCode: child.status,
        signal: child.signal,
        error: child.error ? { code: child.error.code, message: child.error.message } : null,
        validationError,
        result,
        stdout: child.stdout || "",
        stderr: child.stderr || "",
    };
}

if (require.main === module) {
    if (process.argv.includes("--help")) {
        console.log(usage);
    } else {
        try {
            const options = parseOptions(process.argv.slice(2));
            const report = runContract(options);
            mkdirSync(path.dirname(options.output), { recursive: true });
            writeFileSync(options.output, `${JSON.stringify(report, null, 2)}\n`);
            console.log(JSON.stringify(report, null, 2));
            if (!report.validated) process.exitCode = 1;
        } catch (error) {
            console.error(error.stack || String(error));
            process.exitCode = 1;
        }
    }
}

module.exports = { runContract };
