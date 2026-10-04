/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

const assert = require("node:assert/strict");
const { mkdtempSync, rmSync, writeFileSync } = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const { runContract } = require("./run-kafka.cjs");

function runFixture(t, suffix, expectedRuntime = "node", timeoutMs = 5000) {
    const directory = mkdtempSync(path.join(os.tmpdir(), "cookie-cutter-watchdog-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const script = path.join(directory, "child.cjs");
    writeFileSync(
        script,
        `console.log(JSON.stringify({schemaVersion:1, validated:true, runtime:'node', runtimeVersion:process.version}));\n${suffix}\n`
    );
    return runContract(
        { runtime: process.execPath, expectedRuntime, broker: "unused:1", timeoutMs },
        script
    );
}

test("accepts a validated child that exits naturally", (t) => {
    const report = runFixture(t, "");
    assert.equal(report.validated, true);
    assert.equal(report.naturalExit, true);
});

test("rejects success output when an open handle prevents shutdown", (t) => {
    const report = runFixture(t, "setInterval(() => {}, 1000);", "node", 500);
    assert.equal(report.result.validated, true);
    assert.equal(report.validated, false);
    assert.equal(report.naturalExit, false);
    assert.equal(report.error.code, "ETIMEDOUT");
    assert.equal(report.signal, "SIGKILL");
});

test("rejects success output followed by a failing exit", (t) => {
    const report = runFixture(t, "process.exitCode = 1;");
    assert.equal(report.validated, false);
    assert.equal(report.naturalExit, false);
    assert.equal(report.exitCode, 1);
});

test("rejects accidental execution under the wrong runtime", (t) => {
    const report = runFixture(t, "", "bun");
    assert.equal(report.validated, false);
    assert.equal(report.naturalExit, true);
    assert.match(report.validationError, /expected runtime/);
});
