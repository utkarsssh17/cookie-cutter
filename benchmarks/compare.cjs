/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

const { spawnSync } = require("node:child_process");
const { mkdirSync, writeFileSync } = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const usage =
    "Usage: node benchmarks/compare.cjs --node=/path/to/node --bun=/path/to/bun --output=/path/results.json [--samples=5] [--count=100000] [--keys=128] [--mode=concurrent|serial]";

function optionsFrom(argv) {
    const options = { samples: 5, count: 100000, keys: 128, mode: "concurrent" };
    for (const arg of argv) {
        const match = /^--(node|bun|output|samples|count|keys|mode)=(.+)$/.exec(arg);
        if (!match) throw new Error(`Unknown argument: ${arg}\n${usage}`);
        options[match[1]] = match[2];
    }
    for (const name of ["node", "bun", "output"]) {
        if (!options[name]) throw new Error(`Missing --${name}\n${usage}`);
        options[name] = path.resolve(options[name]);
    }
    for (const [name, max] of [
        ["samples", 1000],
        ["count", 10000000],
        ["keys", 1000000],
    ]) {
        options[name] = Number(options[name]);
        if (!Number.isSafeInteger(options[name]) || options[name] < 1 || options[name] > max) {
            throw new Error(`${name} must be an integer between 1 and ${max}`);
        }
    }
    if (!["serial", "concurrent"].includes(options.mode)) {
        throw new Error("mode must be serial or concurrent");
    }
    return options;
}

function stats(values) {
    const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
    if (!sorted.length) return null;
    const middle = Math.floor(sorted.length / 2);
    return {
        median: sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2,
        min: sorted[0],
        max: sorted[sorted.length - 1],
    };
}

function compare(options) {
    const cwd = path.resolve(__dirname, "..");
    const script = path.join(__dirname, "runtime.cjs");
    const invoke = (executable, args) =>
        spawnSync(executable, args, {
            cwd,
            encoding: "utf8",
            timeout: 300000,
            maxBuffer: 1024 * 1024,
        });
    const inspect = (executable, args) => {
        const result = invoke(executable, args);
        return {
            stdout: (result.stdout || "").trim(),
            stderr: (result.stderr || "").trim(),
            exitCode: result.status,
            error: result.error ? result.error.message : null,
        };
    };
    const report = {
        schemaVersion: 1,
        startedAt: new Date().toISOString(),
        options,
        driver: {
            executable: process.execPath,
            version: process.version,
            argv: process.argv.slice(2),
        },
        host: {
            platform: os.platform(),
            release: os.release(),
            arch: os.arch(),
            cpuModels: [...new Set(os.cpus().map((cpu) => cpu.model))],
            logicalCpus: os.cpus().length,
            totalMemoryBytes: os.totalmem(),
            loadAverage: os.loadavg(),
        },
        repository: {
            head: inspect("git", ["rev-parse", "HEAD"]),
            status: inspect("git", ["status", "--porcelain=v1"]),
        },
        versions: {
            node: inspect(options.node, ["--version"]),
            bun: inspect(options.bun, ["--version"]),
        },
        attempts: [],
        summary: {},
    };

    function run(runtime, scenario, phase, sample) {
        const args = [
            script,
            `--scenario=${scenario}`,
            `--mode=${options.mode}`,
            `--count=${scenario === "startup" ? 0 : options.count}`,
            `--keys=${options.keys}`,
        ];
        const startedAt = new Date().toISOString();
        const started = process.hrtime.bigint();
        const child = invoke(options[runtime], args);
        const wallMs = Number(process.hrtime.bigint() - started) / 1e6;
        let result = null;
        let parseError = null;
        try {
            result = JSON.parse(child.stdout || "");
        } catch (error) {
            parseError = error.message;
        }
        const success =
            !child.error &&
            child.status === 0 &&
            result?.validated === true &&
            result.runtime === runtime &&
            result.scenario === scenario &&
            result.mode === options.mode &&
            result.count === (scenario === "startup" ? 0 : options.count) &&
            result.keys === options.keys;
        report.attempts.push({
            runtime,
            scenario,
            phase,
            sample,
            startedAt,
            wallMs,
            success,
            command: [options[runtime], ...args],
            exitCode: child.status,
            signal: child.signal,
            error: child.error ? child.error.message : null,
            parseError,
            result,
            stdout: child.stdout || "",
            stderr: child.stderr || "",
        });
        console.error(
            `${phase} ${scenario} ${runtime} ${sample ?? ""}: ${success ? "ok" : "FAILED"}`
        );
    }

    for (const scenario of ["startup", "stateless", "stateful"]) {
        for (const runtime of ["node", "bun"]) run(runtime, scenario, "rehearsal", null);
        for (let sample = 1; sample <= options.samples; sample++) {
            const order = sample % 2 ? ["node", "bun"] : ["bun", "node"];
            for (const runtime of order) run(runtime, scenario, "measured", sample);
        }
        report.summary[scenario] = {};
        for (const runtime of ["node", "bun"]) {
            const attempts = report.attempts.filter(
                (row) =>
                    row.scenario === scenario && row.runtime === runtime && row.phase === "measured"
            );
            const successful = attempts.filter((row) => row.success);
            const summary = {
                attempted: attempts.length,
                succeeded: successful.length,
                failed: attempts.length - successful.length,
                wallMs: stats(successful.map((row) => row.wallMs)),
            };
            for (const field of [
                "elapsedMs",
                "coreRequireMs",
                "scriptElapsedMs",
                "messagesPerSecond",
                "rssBeforeBytes",
                "rssAfterBytes",
                "rssDeltaBytes",
            ]) {
                summary[field] = stats(successful.map((row) => row.result[field]));
            }
            report.summary[scenario][runtime] = summary;
        }
    }
    report.finishedAt = new Date().toISOString();
    report.failedAttempts = report.attempts.filter((row) => !row.success).length;
    report.validated = report.failedAttempts === 0;
    mkdirSync(path.dirname(options.output), { recursive: true });
    writeFileSync(options.output, `${JSON.stringify(report, null, 2)}\n`);
    console.log(
        JSON.stringify(
            {
                output: options.output,
                validated: report.validated,
                failedAttempts: report.failedAttempts,
                summary: report.summary,
            },
            null,
            2
        )
    );
    if (!report.validated) process.exitCode = 1;
}

if (process.argv.includes("--help")) {
    console.log(usage);
} else {
    try {
        compare(optionsFrom(process.argv.slice(2)));
    } catch (error) {
        console.error(error.stack || String(error));
        process.exitCode = 1;
    }
}
