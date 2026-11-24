#!/usr/bin/env bash
if [[ -f "$ROOT/cluster.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/cluster.env"
fi

