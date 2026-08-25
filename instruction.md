# Consistent recovery across mutable replica storage

This repository is based on Litestream v0.5.11, a real SQLite streaming disaster-recovery project. The starter contains a set of related consistency defects in WAL synchronization, ranged object reads, initial restore, and continuous restore. Repair the implementation without changing its public APIs or the LTX wire format.

Your implementation must satisfy all of the following behavior:

1. A WAL file can be checkpointed and reused without growing. A WAL whose byte length equals the last synchronized end offset is not necessarily idle: if its identity changed, the new committed state must produce the next LTX transaction rather than being silently skipped.
2. Ranged LTX reads are byte-exact across connection loss. Reopening a stream after a partial read must neither duplicate nor omit a byte. Cancellation and permanent backend failures must return promptly.
3. Retention may delete an object after a restore plan is listed but before that object is opened. When another current compaction path can still reach the same target transaction, restore must make a bounded fresh plan and succeed. The retry must remain pinned to the original target and must not advance to a transaction that appeared later. If no current plan reaches the target, fail promptly.
4. A new restore is published atomically. The destination path must not become visible until decoding, durability, and the requested integrity check have all succeeded. A failed or cancelled restore must leave neither the destination nor restore-temporary artifacts. Concurrent restores must not share a fixed temporary filename.
5. A continuous-restore update is atomic with respect to validation. A truncated or corrupt LTX file must not partially modify the visible database or advance its matching TXID. A later retry with the valid compacted file must be able to advance normally.
6. Preserve existing destination-exists refusal, restore target and timestamp semantics, SQLite header normalization, checksum validation, and context cancellation.

## Scope and constraints

- Work only in `/app`.
- Do not modify `/app/repro`, `/app/fixtures`, `/tests`, or `/solution`.
- Do not add network-dependent behavior or replace the LTX decoder/compactor with a task-specific parser.
- Individual verifier fixtures are below 32 MiB.
- Preserve the module path and the public method signatures used by existing callers.

## Diagnostics

Run the deterministic public reproducer at any time:

```bash
/app/repro/run-all.sh
```

It prints at least three uniquely named JSON scenarios, one object per line, with exactly these keys: `scenario` (non-empty string), `ok` (boolean), `txid` (an unsigned decimal integer encoded as a string), and `detail` (non-empty string). A successful run reports `ok: true` for every scenario. The starter is expected to report failures. The hidden verifier also exercises adversarial offsets, same-size WAL reuse, missing restore alternatives, corrupt incremental files, cleanup, and a valid retry.

The final verifier builds the Litestream command and runs focused Go tests. All required dependencies are already present in the image; no internet access is needed.
