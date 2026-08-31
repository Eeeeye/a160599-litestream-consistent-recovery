# Crash-consistent recovery across mutable replica storage

This repository is based on the real Litestream v0.5.11 SQLite disaster-recovery codebase. The starter has related consistency defects across WAL identity detection, ranged object reads, restore planning, pathname publication, and continuous-follow state. Repair the implementation without changing existing public APIs or the LTX wire format.

Your implementation must satisfy all of these behaviors:

1. **Equal-length WAL replacement:** checkpoint/restart can replace a WAL and refill it to the previous byte length. Equal length is idle only for the same WAL identity; a replacement containing a new commit must produce the next LTX transaction.
2. **Range results and liveness:** for any finite sequence of short reads and transient interruptions, the bytes returned for an advertised range must exactly equal that range, with no duplicate or missing byte. One lifetime-wide budget contains exactly three retry credits; the initial stream is not a retry. Each resumable interruption and each transient reopen failure consumes exactly one credit. A read that returns both bytes and a resumable error first delivers those bytes and still consumes one credit for that error. Credits never reset after successful reads or between `Read` calls. The event that would consume a fourth credit is terminal, and subsequent reads return that same failure. Independently, `(0, nil)` results consume no retry credits: at most three consecutive such results are tolerated after any positive progress, and the fourth is terminal with `io.ErrNoProgress`. Positive progress resets only this no-progress counter, never the retry-credit budget. Reject advertised-size overruns and fail promptly on cancellation, missing objects, or permanent errors.
3. **Restore selection under retention:** an object can disappear after listing, on initial open, or after some of its bytes were read. A restore may reconsider the currently available objects, but the target TXID selected by that invocation cannot change when newer files appear. Attempts must be bounded, and failure is required when the selected target is no longer reachable.
4. **Concurrent destination semantics:** the destination must remain absent until decoding, synchronization, and any requested integrity check have all succeeded. Failed or cancelled work leaves no restore artifacts. If concurrent restores target one absent pathname, exactly one may succeed; every loser must preserve the winner byte-for-byte and report destination-exists behavior.
5. **Poll visibility:** one poll can contain several contiguous LTX files or a higher-level gap fill. The database bytes and recorded TXID visible before the poll must remain unchanged if any member is corrupt, truncated, cancelled, or missing. When all members are valid, both advance to the poll's final generation, and a corrected retry after failure must still succeed.
6. **Restart consistency:** the followed database and `<db>-txid` are one logical generation although they occupy separate files. The durable recovery-state pathname is `<db>-follow`. It contains exactly one JSON v1 object, is terminated by a newline, and has no trailing JSON value or non-whitespace data. The object has exactly these eight fields—no missing or unknown fields—and their JSON types must match: `version` (integer `1`), `from_txid` and `to_txid` (exactly 16 lowercase hexadecimal digits with `to_txid > from_txid`), `stage` (string), `old_size` and `new_size` (integer byte counts), and `old_sha256` and `new_sha256` (strings). `new_size` is non-negative and `new_sha256` is a lowercase 64-digit full-file SHA-256. For initial publication, `from_txid` is zero, `old_size` is exactly `-1`, and `old_sha256` is empty; for a later poll, `from_txid` is nonzero, `old_size` is non-negative, and `old_sha256` is also a lowercase 64-digit full-file SHA-256.

   Here `<db-base>` means `filepath.Base(outputPath)`, including its filename extension and excluding directory components (for example, `/tmp/recover.db` has `<db-base>` `recover.db`). `stage` must be a basename with no directory or traversal component and must start with `.<db-base>-restore-` for initial publication or `.<db-base>-follow-` for a later poll; the opposite prefix is invalid. Record publication may stage through `.<db-base>-follow-record-*`; per-LTX staging may use `.litestream-follow-*`; TXID publication uses `<db>-txid.tmp`.

   Recovery must reconcile the visible database identity and sidecar exactly. If the visible database matches the recorded new size and SHA-256, the sidecar may equal only `from_txid` or `to_txid`; advance it from `from_txid` to `to_txid` if needed. If the visible database matches the recorded old identity, or an initial generation is still unpublished, the sidecar must equal `from_txid` and recovery discards the unpublished stage. Thus a sidecar outside `{from_txid, to_txid}` is always invalid for the new visible identity, and any sidecar other than `from_txid` is invalid for the old or unpublished identity. Any other sidecar value, database identity, field value, field type, field set, record termination, TXID ordering, old-identity combination, or stage pathname/prefix is malformed and must fail closed: preserve the visible database, sidecar, outside paths, and evidence record without advancing the sidecar. Successful initial publication, poll publication, or recovery removes every listed temporary and recovery artifact.
7. Preserve destination-exists refusal, TXID/timestamp selection, higher-level gap fill, LTX checksum validation, SQLite header normalization, and context cancellation. A later-poll follower publication must preserve the existing database's exact permission bits, including mode `0640` when that is the pre-update mode.

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
