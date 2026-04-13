#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/c/Users/oaolive/repos/mobile-app"
TARGET_BRANCH="relocate-archive"
RETURN_BRANCH="master"

cd "$REPO_DIR"

cleanup() {
  git checkout "$RETURN_BRANCH"
}

trap cleanup EXIT

git rev-parse --is-inside-work-tree >/dev/null 2>&1

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Working tree has uncommitted changes. Commit or stash them first."
  exit 1
fi

git checkout "$TARGET_BRANCH"

Rscript "R/update_base_tables.R"
Rscript "R/update_app_source_tables.R"