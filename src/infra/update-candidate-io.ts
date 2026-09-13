import fs from "node:fs/promises";
import path from "node:path";
import { scheduleAbsoluteDeadline } from "../utils/absolute-deadline.js";
import { sleep } from "../utils/sleep.js";
import { formatDiskSpaceBytes } from "./disk-space.js";
import { hasNodeErrorCode } from "./path-guards.js";
import { resolveSqliteInspectionBudget } from "./sqlite-readonly-worker.js";

export async function measureUpdateStateFiles(
  files: Iterable<string>,
): Promise<{ bytes: number; largest: number }> {
  let bytes = 0;
  let largest = 0;
  for (const file of files) {
    let family = 0;
    for (const suffix of ["", "-wal", "-shm", "-journal"]) {
      try {
        family += (await fs.stat(file + suffix)).size;
      } catch (error) {
        if (!hasNodeErrorCode(error, "ENOENT")) {
          throw error;
        }
      }
    }
    bytes += family;
    largest = Math.max(largest, family);
  }
  return { bytes, largest };
}

async function inspectCopyProgress(
  directory: string,
  signal: AbortSignal,
): Promise<{ facts: string; bytes: number }> {
  const facts: string[] = [];
  let bytes = 0;
  async function visit(current: string): Promise<void> {
    signal.throwIfAborted();
    for (const entry of await fs.readdir(current, { withFileTypes: true })) {
      signal.throwIfAborted();
      const file = path.join(current, entry.name);
      try {
        if (entry.isDirectory()) {
          await visit(file);
        } else if (entry.isFile()) {
          const stat = await fs.stat(file);
          bytes += stat.size;
          facts.push(`${file}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`);
        }
      } catch (error) {
        // Completed intermediate copies can disappear while the worker advances.
        if (!hasNodeErrorCode(error, "ENOENT")) {
          throw error;
        }
      }
    }
  }
  await visit(directory);
  return { facts: facts.toSorted().join("\n"), bytes };
}

/** One IO watchdog for private state workers; callers retain child and scratch ownership. */
export async function withUpdateCandidateIoBudget<T>(
  params: {
    directory: string;
    bytes: number;
    timeoutMs?: number;
    signal?: AbortSignal;
    operation?: "snapshot" | "inspection";
  },
  run: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  params.signal?.throwIfAborted();
  // Each period without observable progress receives the shared SQLite IO allowance.
  const budgetFor = (bytes: number) =>
    Math.max(
      params.timeoutMs ?? 0,
      resolveSqliteInspectionBudget(
        `update state ${params.operation ?? "inspection"}`,
        params.directory,
        bytes,
      ).timeoutMs,
    );
  let knownBytes = params.bytes;
  let budget = budgetFor(knownBytes);
  let deadline = Date.now() + budget;
  let previous: string | undefined;
  const stalled = new AbortController();
  const finished = new AbortController();
  const signal = AbortSignal.any([stalled.signal, ...(params.signal ? [params.signal] : [])]);
  const monitorSignal = AbortSignal.any([signal, finished.signal]);
  const expire = () =>
    stalled.abort(
      new Error(
        `Update state ${params.operation ?? "inspection"} made no progress for ${budget / 1000} seconds (${formatDiskSpaceBytes(knownBytes)} of SQLite state). Check storage performance before retrying.`,
      ),
    );
  let cancelDeadline = scheduleAbsoluteDeadline(deadline, expire);
  // Deadline enforcement cannot wait for filesystem metadata, including the first scan.
  void (async () => {
    try {
      while (!monitorSignal.aborted) {
        const current = await inspectCopyProgress(params.directory, monitorSignal);
        monitorSignal.throwIfAborted();
        if (Date.now() >= deadline) {
          expire();
          return;
        }
        if (current.bytes > knownBytes || (previous !== undefined && current.facts !== previous)) {
          // Registered external databases may first become visible inside the worker.
          knownBytes = Math.max(knownBytes, current.bytes);
          budget = budgetFor(knownBytes);
          deadline = Date.now() + budget;
          cancelDeadline();
          cancelDeadline = scheduleAbsoluteDeadline(deadline, expire);
        }
        previous = current.facts;
        await sleep(1_000, monitorSignal);
      }
    } catch (error) {
      if (!monitorSignal.aborted) {
        stalled.abort(error);
      }
    }
  })();
  try {
    const result = await run(signal);
    if (Date.now() >= deadline) {
      expire();
    }
    signal.throwIfAborted();
    return result;
  } finally {
    finished.abort();
    cancelDeadline();
  }
}
