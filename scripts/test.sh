#!/usr/bin/env bash

# Runs user/integration tests.

set -e

PROJECT_ROOT="$(git rev-parse --show-toplevel)"
TMP_DIRS_FILE=/tmp/p9fs-dirs

PY9P_PID=/tmp/py9p.pid
PY9P_LOG=/tmp/py9p.log

UNPFS_PID=/tmp/unpfs.pid
UNPFS_LOG=/tmp/unpfs.log

export PYTHONUNBUFFERED=1

function mymktempdir {
  local tmpdir="$(mktemp -d)"
  echo "$tmpdir" | tee -a $TMP_DIRS_FILE
}

function wait_for_server {
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
  wait_for_server
}

function stop_py9p {
  if [[ -f $PY9P_PID ]]; then
    kill "$(<$PY9P_PID)" || true
    rm -f "$PY9P_PID"
  fi
}

function run_unpfs {
  local exported_dir="$1"
  ${UNPFS:-unpfs} 'tcp!0.0.0.0!1234' "$exported_dir" &> $UNPFS_LOG &
  echo $! > $UNPFS_PID
  wait_for_server
}

function stop_unpfs {
  if [[ -f $UNPFS_PID ]]; then
    kill "$(<$UNPFS_PID)" || true
    rm -f "$UNPFS_PID"
  fi
}

function user_tests {
  cd "$PROJECT_ROOT/src"
  python -m pytest -v -rA ../tests/user "$@"
}

function cleanup {
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
stop_py9p

echo "Testing py9p server with 9P2000.u"
EXPORTED_DIR=""$(mymktempdir)""
run_py9p "$EXPORTED_DIR" 9P2000.u
user_tests --exported "$EXPORTED_DIR" --9p 9P2000.u
stop_py9p

echo "Testing unpfs server with 9P2000.L"
EXPORTED_DIR=""$(mymktempdir)""
run_unpfs "$EXPORTED_DIR"
user_tests --exported "$EXPORTED_DIR" --9p 9P2000.L
stop_unpfs
