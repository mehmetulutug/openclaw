import fs from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { withUpdateCandidateIoBudget } from "./update-candidate-io.js";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

it.each(
  ["initial stat", "later stat", "initial readdir"].flatMap((poll) =>
    ["deadline", "cancellation", "completion"].map((outcome) => ({ poll, outcome })),
  ),
)("settles $outcome while the $poll never resolves", async ({ poll, outcome }) => {
  const directory = tempDirs.make("openclaw-io-watchdog-");
  const copy = path.join(directory, "database.sqlite");
  await fs.writeFile(copy, "partial");
  const firstRead = createDeferred();
  const blocked = createDeferred();
  const workerExit = createDeferred();
  const stat = fs.stat;
  let reads = 0;
  vi.spyOn(fs, "stat").mockImplementation(async (file, options) => {
    if (file !== copy) {
      return stat(file, options);
    }
    if (poll === "later stat" && reads++ === 0) {
      const result = await stat(file, options);
      firstRead.resolve();
      return result;
    }
    blocked.resolve();
    return new Promise<never>(() => {});
  });
  if (poll === "initial readdir") {
    vi.spyOn(fs, "readdir").mockImplementation(() => {
      blocked.resolve();
      return new Promise<never>(() => {});
    });
  }
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  const startedAt = Date.now();
  const controller = new AbortController();
  let workerSignal: AbortSignal | undefined;
  let settled: { result: string } | { error: unknown } | undefined;
  void withUpdateCandidateIoBudget(
    { directory, bytes: 32 * 1024 ** 2, signal: controller.signal },
    async (signal) => {
      workerSignal = signal;
      const onAbort = () => workerExit.resolve();
      signal.addEventListener("abort", onAbort, { once: true });
      try {
        await workerExit.promise;
        return "worker exited";
      } finally {
        signal.removeEventListener("abort", onAbort);
      }
    },
  ).then(
    (result) => {
      settled = { result };
    },
    (error: unknown) => {
      settled = { error };
    },
  );
  try {
    if (poll === "later stat") {
      await firstRead.promise;
      await vi.advanceTimersByTimeAsync(1_000);
    }
    await blocked.promise;
    expect(settled).toBeUndefined();
    const cancellation = new Error("cancel inspection");
    if (outcome === "deadline") {
      await vi.advanceTimersByTimeAsync(340_000 - (Date.now() - startedAt));
    } else if (outcome === "cancellation") {
      controller.abort(cancellation);
    } else {
      workerExit.resolve();
    }
    await vi.advanceTimersByTimeAsync(0);
    if (outcome === "completion") {
      expect(settled).toEqual({ result: "worker exited" });
    } else {
      expect(workerSignal?.aborted).toBe(true);
      expect(settled).toEqual({
        error:
          outcome === "cancellation"
            ? cancellation
            : expect.objectContaining({
                message: expect.stringContaining(
                  "made no progress for 340 seconds (32 MiB of SQLite state)",
                ),
              }),
      });
    }
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    controller.abort();
    workerExit.resolve();
    vi.clearAllTimers();
    vi.restoreAllMocks();
  }
});

it("checks the completion deadline before its timer callback runs", async () => {
  const directory = tempDirs.make("openclaw-io-completion-");
  const entered = createDeferred();
  const exit = createDeferred();
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  const operation = withUpdateCandidateIoBudget({ directory, bytes: 32 * 1024 ** 2 }, () => {
    entered.resolve();
    return exit.promise;
  }).then(
    () => ({ error: undefined }),
    (error: unknown) => ({ error }),
  );
  await entered.promise;
  vi.setSystemTime(Date.now() + 340_000);
  exit.resolve();
  await vi.advanceTimersByTimeAsync(0);
  expect(await operation).toMatchObject({
    error: { message: expect.stringContaining("made no progress for 340 seconds") },
  });
  expect(vi.getTimerCount()).toBe(0);
});

it("does not continue traversal when a metadata read resolves after cancellation", async () => {
  const directory = tempDirs.make("openclaw-io-late-read-");
  await fs.writeFile(path.join(directory, "first.sqlite"), "partial");
  await fs.writeFile(path.join(directory, "second.sqlite"), "partial");
  const metadata = await fs.stat(path.join(directory, "first.sqlite"));
  const read = createDeferred<typeof metadata>();
  const blocked = createDeferred();
  const exited = createDeferred();
  const stat = fs.stat;
  const calls: string[] = [];
  vi.spyOn(fs, "stat").mockImplementation((file, options) => {
    if (typeof file !== "string" || path.dirname(file) !== directory) {
      return stat(file, options);
    }
    calls.push(file);
    blocked.resolve();
    return read.promise;
  });
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  const controller = new AbortController();
  controller.signal.addEventListener("abort", () => exited.resolve(), { once: true });
  let error: unknown;
  const operation = withUpdateCandidateIoBudget(
    { directory, bytes: 4096, signal: controller.signal },
    () => exited.promise,
  ).catch((cause: unknown) => {
    error = cause;
  });
  try {
    await blocked.promise;
    const cancellation = new Error("cancel during metadata read");
    controller.abort(cancellation);
    await vi.advanceTimersByTimeAsync(0);
    expect(error).toBe(cancellation);
    read.resolve(metadata);
    await vi.advanceTimersByTimeAsync(0);
    expect(calls).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    controller.abort();
    read.resolve(metadata);
    await operation;
    vi.clearAllTimers();
    vi.restoreAllMocks();
  }
});
