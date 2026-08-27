# Crash-consistent recovery across mutable replica storage

This repository is based on the real Litestream v0.5.11 SQLite disaster-recovery codebase. The starter has related consistency defects across WAL identity detection, ranged object reads, restore planning, pathname publication, and continuous-follow state. Repair the implementation without changing existing public APIs or the LTX wire format.

Your implementation must satisfy all of these behaviors:

1. **Equal-length WAL replacement:** checkpoint/restart can replace a WAL and refill it to the previous byte length. Equal length is idle only for the same WAL identity; a replacement containing a new commit must produce the next LTX transaction.
2. **Range results and liveness:** for any finite sequence of short reads and transient interruptions, the bytes returned for an advertised range must exactly equal that range, with no duplicate or missing byte. One retry allowance applies to the reader's complete lifetime, including interruptions reported together with bytes and failures while resuming; once exhausted, subsequent reads return the same terminal failure. Reject advertised-size overruns, bound repeated `(0, nil)` results, and fail promptly on cancellation, missing objects, or permanent errors.
3. **Restore selection under retention:** an object can disappear after listing, on initial open, or after some of its bytes were read. A restore may reconsider the currently available objects, but the target TXID selected by that invocation cannot change when newer files appear. Attempts must be bounded, and failure is required when the selected target is no longer reachable.
4. **Concurrent destination semantics:** the destination must remain absent until decoding, synchronization, and any requested integrity check have all succeeded. Failed or cancelled work leaves no restore artifacts. If concurrent restores target one absent pathname, exactly one may succeed; every loser must preserve the winner byte-for-byte and report destination-exists behavior.
5. **Poll visibility:** one poll can contain several contiguous LTX files or a higher-level gap fill. The database bytes and recorded TXID visible before the poll must remain unchanged if any member is corrupt, truncated, cancelled, or missing. When all members are valid, both advance to the poll's final generation, and a corrected retry after failure must still succeed.
6. **Restart consistency:** the followed database and `<db>-txid` are one logical generation although they occupy separate files. The durable recovery-state pathname is `<db>-follow`; its encoding is implementation-private. If execution stops after the new database becomes visible but before its TXID does, the record must remain and a later invocation must complete the sidecar update only when the visible database exactly matches the recorded new generation. An unpublished update must leave the old generation visible. A mismatched database, tampered recovery state, or impossible sidecar value must fail closed without advancing the sidecar. Successful initial publication, poll publication, or recovery removes all temporary and recovery artifacts.
7. Preserve destination-exists refusal, TXID/timestamp selection, higher-level gap fill, LTX checksum validation, SQLite header normalization, file modes, and context cancellation.

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

It prints exactly five uniquely named JSON scenarios—`resume_exact`, `resume_budget`, `retention_replan`, `failure_cleanup`, and `initial_follow_recovery`—one object per line, with exactly these keys: `scenario` (non-empty string), `ok` (boolean), `txid` (an unsigned decimal integer encoded as a string), and `detail` (non-empty string). A successful run reports `ok: true` for every scenario. The starter is expected to fail. Hidden tests add adversarial offsets, no-progress streams, mid-batch corruption, concurrent publication, recovery tampering, missing alternatives, cleanup, and valid retries.

The final verifier builds the Litestream command and runs focused Go tests. All required dependencies are already present in the image; no internet access is needed.
