#!/usr/bin/env bash

# Runs user/integration tests.

set -e

PROJECT_ROOT="$(git rev-parse --show-toplevel)"
TMP_DIRS_FILE=/tmp/p9fs-dirs
PY9P_PID=/tmp/py9p.pid
PY9P_LOG=/tmp/py9p.log

export PYTHONUNBUFFERED=1

function mymktempdir {
  local tmpdir="$(mktemp -d)"
  echo "$tmpdir" | tee -a $TMP_DIRS_FILE
}

function run_py9p {
  local exported_dir="$1"
  local protocol="$2"

  args=(
    -p 1234
    -w
    -d
    -r "$exported_dir"
  )

  if [[ $protocol == "9P2000.u" ]]; then
    args+=( -D )
  fi

  cd "$PROJECT_ROOT/src"
  python -m py9p "${args[@]}" &> $PY9P_LOG &
  echo $! > $PY9P_PID

  echo Waiting for 9p server ...
  SERVER_READY=""
  for i in $(seq 1 5); do
    sleep 1
    if nc -d -w 1 -z 127.0.0.1 1234; then
      SERVER_READY="true"
      break
    fi
  done
  if [[ ! $SERVER_READY ]]; then
    echo 9p server failed to start
    exit 1
  fi
}

function user_tests {
  cd "$PROJECT_ROOT/src"
  python -m pytest -v -rA ../tests/user "$@"
}

function cleanup {
  if [[ -f $PY9P_PID ]]; then
    kill "$(<$PY9P_PID)" || true
    rm -f "$PY9P_PID"
  fi
  if [[ -f $TMP_DIRS_FILE ]]; then
    xargs -n1 -r rm -rf < $TMP_DIRS_FILE
    rm -f $TMP_DIRS_FILE
  fi
}

trap cleanup EXIT

rm -f "$TMP_DIRS_FILE"

echo "PROJECT_ROOT: $PROJECT_ROOT"

echo "Testing py9p server with 9P2000"
EXPORTED_DIR=""$(mymktempdir)""
run_py9p "$EXPORTED_DIR" 9P2000
user_tests --exported "$EXPORTED_DIR" --9p 9P2000
kill "$(<$PY9P_PID)"; rm "$PY9P_PID"

echo "Testing py9p server with 9P2000.u"
EXPORTED_DIR=""$(mymktempdir)""
run_py9p "$EXPORTED_DIR" 9P2000.u
user_tests --exported "$EXPORTED_DIR" --9p 9P2000.u
kill "$(<$PY9P_PID)"; rm "$PY9P_PID"
