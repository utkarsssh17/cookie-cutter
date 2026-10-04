/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

import { Application, ErrorHandlingMode, ParallelismMode, StaticInputSource } from "..";
import {
    NullLogger,
    NullMetrics,
    NullOutputSink,
    NullStateProvider,
    NullTracerBuilder,
} from "../defaults";
import { IPublishedMessage } from "../model";

describe("Application shutdown", () => {
    beforeEach(() => {
        jest.spyOn(process, "on").mockReturnValue(process);
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    const disposalOrder = ["source", "sink", "service", "state", "metrics", "tracer"];

    function createApp(failures: string[] = []) {
        const calls: string[] = [];
        const dispose = (name: string) => ({
            dispose: jest.fn(async () => {
                calls.push(`start ${name}`);
                await Promise.resolve();
                calls.push(`finish ${name}`);
                if (failures.includes(name)) {
                    throw new Error(`${name} disposal failed`);
                }
            }),
        });
        const logger = new NullLogger();
        const logError = jest.spyOn(logger, "error");
        const app = Application.create()
            .input()
            .add(Object.assign(new StaticInputSource([]), dispose("source")))
            .done()
            .output()
            .published(Object.assign(new NullOutputSink<IPublishedMessage>(), dispose("sink")))
            .done()
            .services()
            .add("service", dispose("service"))
            .done()
            .state(Object.assign(new NullStateProvider(), dispose("state")))
            .metrics(Object.assign(new NullMetrics(), dispose("metrics")))
            .tracer(Object.assign(new NullTracerBuilder(), dispose("tracer")))
            .logger(logger);

        return { app, calls, logError };
    }

    it("awaits every component disposal in order when shutdown succeeds", async () => {
        const { app, calls, logError } = createApp();

        await expect(app.run(ErrorHandlingMode.LogAndFail, ParallelismMode.Serial)).resolves.toBe(
            undefined
        );

        expect(calls).toEqual(disposalOrder.flatMap((name) => [`start ${name}`, `finish ${name}`]));
        expect(logError).not.toHaveBeenCalled();
    });

    it.each([{ failures: ["source"] }, { failures: ["state"] }, { failures: ["source", "state"] }])(
        "continues cleanup and reports failure when $failures fail",
        async ({ failures }) => {
            const { app, calls, logError } = createApp(failures);

            await expect(
                app.run(ErrorHandlingMode.LogAndFail, ParallelismMode.Serial)
            ).rejects.toThrow("test failed: init: true, run: true, dispose: false");

            expect(calls).toEqual(
                disposalOrder.flatMap((name) => [`start ${name}`, `finish ${name}`])
            );
            expect(logError).toHaveBeenCalledTimes(failures.length);
            for (const failure of failures) {
                expect(logError).toHaveBeenCalledWith(
                    "failed to dispose component",
                    new Error(`${failure} disposal failed`)
                );
            }
        }
    );
});
