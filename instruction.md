# Crash-consistent recovery across mutable replica storage

This repository is based on the real Litestream v0.5.11 SQLite disaster-recovery codebase. The starter has related consistency defects across WAL identity detection, ranged object reads, restore planning, pathname publication, and continuous-follow state. Repair the implementation without changing existing public APIs or the LTX wire format.

Your implementation must satisfy all of these behaviors:

1. **Same-size WAL reuse:** checkpoint/restart can replace a WAL and refill it to the previous byte length. Equality with the last synchronized offset is only idle when the WAL identity still matches; a changed identity with a new commit must produce the next LTX transaction.
2. **Bounded byte-exact ranges:** after any partial read, reopen at the first unread byte—never overlap or skip. A single retry budget spans the whole reader, including failures returned with bytes and reopen failures; exhaustion is sticky. Reject advertised-size overruns and bound repeated `(0, nil)` reads. Cancellation, missing objects, and permanent errors fail promptly.
3. **Pinned retention replanning:** an object can disappear after listing, on initial open, or during a resumed range. Retry the complete restore with a fresh current plan, but keep the first selected TXID frozen even if newer files appear. Attempts are bounded; fail promptly if no plan reaches that exact target.
4. **Single-winner initial publication:** decode, sync, and any requested integrity check must finish on a unique same-directory stage before the destination name is visible. Failed/cancelled work leaves no restore artifacts. Concurrent restores to one absent path must yield exactly one publisher; a loser must not overwrite the winner.
5. **All-or-nothing follow batches:** one poll can contain several contiguous LTX files or a higher-level gap fill. Validate and durably stage the entire batch before changing the visible database. If any later file is corrupt, truncated, cancelled, or missing, neither earlier files from that batch nor its TXID may become visible. A corrected retry must still advance.
6. **Recoverable database/TXID commit:** the followed database and `<db>-txid` form one logical generation although they are separate files. Both initial follow publication and later batches need a bounded durable recovery record. After interruption between database publication and TXID publication, restart must compare the visible file with recorded old/new identities, roll forward only on an exact new-generation match, roll back an unpublished stage, and fail closed on tampering or an impossible sidecar value. Successful recovery removes its stage/record artifacts.
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
