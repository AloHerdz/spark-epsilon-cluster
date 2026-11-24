#!/usr/bin/env bash
if [[ -f "$ROOT/cluster.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/cluster.env"
fi
[[ -z ${JAVA_HOME:-} ]] || export JAVA_HOME
[[ -z ${SPARK_DOWNLOAD_BASE:-} ]] || export SPARK_DOWNLOAD_BASE

worker_aliases() {
  local item
  IFS=',' read -r -a _worker_aliases <<< "${WORKER_HOSTS:-}"
  for item in "${_worker_aliases[@]}"; do
    [[ -n $item ]] && printf '%s\n' "$item"
  done
}

worker_ip_for() {
  local target=$1 pair
  IFS=',' read -r -a _worker_ip_pairs <<< "${WORKER_IPS:-}"
  for pair in "${_worker_ip_pairs[@]}"; do
    if [[ ${pair%%=*} == "$target" ]]; then
      printf '%s\n' "${pair#*=}"
      return 0
    fi
  done
  return 1
}
