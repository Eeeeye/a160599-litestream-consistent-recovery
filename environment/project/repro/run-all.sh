#!/bin/bash
set -uo pipefail
cd /app || exit 1
export GOTOOLCHAIN=local
export GOPROXY=off
export GOSUMDB=off
exec go run ./repro
