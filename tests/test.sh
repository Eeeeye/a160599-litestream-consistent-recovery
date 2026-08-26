#!/bin/bash
set -uo pipefail

mkdir -p /logs/verifier
echo 0 > /logs/verifier/reward.txt

root_test=/app/zz_activity160599_test.go
internal_test=/app/internal/zz_activity160599_internal_test.go
cleanup() {
  rm -f "$root_test" "$internal_test"
}
trap cleanup EXIT

cp /tests/activity160599_root_test.go "$root_test"
cp /tests/activity160599_internal_test.go "$internal_test"

cd /app || exit 1
export GOTOOLCHAIN=local
export GOPROXY=off
export GOSUMDB=off

if ! timeout 180s go test -count=1 -run '^TestActivity160599' . ./internal; then
  exit 1
fi

if ! timeout 240s go test -race -count=1 -run '^TestActivity160599(ConcurrentRestoresPublishExactlyOnce|FollowerBatchIsAllOrNothing|FollowerCommitRecoveryIsIdentityChecked|InitialFollowPublicationRecoversMissingTXID|ResumableReader)' . ./internal; then
  exit 1
fi

repro_log=/tmp/activity160599-repro.jsonl
if ! timeout 90s /app/repro/run-all.sh > "$repro_log"; then
  cat "$repro_log"
  exit 1
fi
cat "$repro_log"

if ! timeout 30s go run /tests/validate_repro.go "$repro_log"; then
  echo "reproducer emitted an invalid schema or a failing scenario" >&2
  exit 1
fi

if ! timeout 180s go build -o /tmp/litestream-activity160599 ./cmd/litestream; then
  exit 1
fi

echo 1 > /logs/verifier/reward.txt
