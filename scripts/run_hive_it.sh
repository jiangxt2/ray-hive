#!/usr/bin/env bash
# Copyright 2026 ray-hive contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${project_root}/tests/integration/docker-compose.yml"
log_dir="${RAY_HIVE_IT_LOG_DIR:-${project_root}/tests/integration/logs}"
hive_image="${RAY_HIVE_IT_HIVE_IMAGE:-docker.m.daocloud.io/apache/hive@sha256:b0d2842722567a89ac35da8af9002b6bba0fc2d0eb272664954a1fa46d694472}"
export RAY_HIVE_IT_HIVE_IMAGE="${hive_image}"

mkdir -p "${log_dir}"

record_docker_state() {
  local suffix="$1"
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker image ls --filter dangling=true --no-trunc \
    > "${log_dir}/dangling-images-${suffix}.log" 2>&1 || true
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker image inspect "${hive_image}" \
    > "${log_dir}/hive-image-${suffix}.log" 2>&1 || true
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker system df > "${log_dir}/docker-system-df-${suffix}.log" 2>&1 || true
}

capture_logs() {
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker compose -p ray-hive-it -f "${compose_file}" logs --no-color \
    > "${log_dir}/hive-420.log" 2>&1 || true
}

cleanup() {
  capture_logs
  record_docker_state after-compose
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    docker compose -p ray-hive-it -f "${compose_file}" down -v --remove-orphans
  record_docker_state after-cleanup
}

trap cleanup EXIT

record_docker_state before
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -p ray-hive-it -f "${compose_file}" up -d --wait --no-build
RAY_HIVE_HS2_HOST=127.0.0.1 \
RAY_HIVE_HS2_PORT="${RAY_HIVE_HS2_PORT:-10090}" \
  "${project_root}/.venv/bin/python" "${project_root}/scripts/setup_hive_it.py" \
  "${project_root}/tests/integration/setup.sql"

RAY_HIVE_HS2_HOST=127.0.0.1 \
RAY_HIVE_HS2_PORT="${RAY_HIVE_HS2_PORT:-10090}" \
  "${project_root}/.venv/bin/python" -m pytest "${project_root}/tests/integration" \
  -m "not cluster" -v
