#!/bin/bash
set -euo pipefail

cd /app
git apply --unidiff-zero --check /solution/solution.patch
git apply --unidiff-zero /solution/solution.patch
/usr/local/go/bin/gofmt -w /app/db.go /app/replica.go /app/internal/resumable_reader.go
