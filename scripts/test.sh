#!/usr/bin/env bash

# Runs user/integration tests.

set -e

PROJECT_ROOT="$(git rev-parse --show-toplevel)"
TMP_DIRS_FILE=/tmp/p9fs-dirs
PY9S_SERVER_PID=/tmp/p9fs.pid
PY9S_SERVER_LOG=/tmp/p9fs.log

function mymktempdir {
  local tmpdir="$(mktemp -d)"
  echo "$tmpdir" | tee -a $TMP_DIRS_FILE
}

function run_py9s_server {
  local exported_dir="$1"
  cd "$PROJECT_ROOT/src"
  python -m py9p -p 1234 -w -d -r "$exported_dir" &> $PY9S_SERVER_LOG &
  echo $! > $PY9S_SERVER_PID

  echo Waiting for 9p server ...
  SERVER_READY=""
  for i in $(seq 1 5); do
    if nc -d -w 1 -z 127.0.0.1 1234; then
      SERVER_READY="true"
      break
    fi
    sleep 1
  done
  if [[ ! $SERVER_READY ]]; then
    echo 9p server failed to start
    exit 1
  fi
}

function user_tests {
  local exported_dir="$1"
  cd "$PROJECT_ROOT/src"
  python -m pytest -v -rA ../tests/user --exported "$exported_dir"
}

function cleanup {
  if [[ -f $PY9S_SERVER_PID ]]; then
    kill "$(<$PY9S_SERVER_PID)" || true
    rm -f "$PY9S_SERVER_PID"
  fi
  if [[ -f $TMP_DIRS_FILE ]]; then
    xargs -n1 -r rm -rf < $TMP_DIRS_FILE
    rm -f $TMP_DIRS_FILE
  fi
}

trap cleanup EXIT

rm -f "$TMP_DIRS_FILE"

EXPORTED_DIR=""$(mymktempdir)""
run_py9s_server "$EXPORTED_DIR"
user_tests "$EXPORTED_DIR"
