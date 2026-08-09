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

mkdir -p "${log_dir}"

capture_logs() {
  docker compose -p ray-hive-it -f "${compose_file}" logs --no-color \
    > "${log_dir}/hive-420.log" 2>&1 || true
}

cleanup() {
  capture_logs
  docker compose -p ray-hive-it -f "${compose_file}" down -v --rmi local
}

trap cleanup EXIT

docker compose -p ray-hive-it -f "${compose_file}" up -d --wait
RAY_HIVE_HS2_HOST=127.0.0.1 \
RAY_HIVE_HS2_PORT="${RAY_HIVE_HS2_PORT:-10090}" \
  "${project_root}/.venv/bin/python" "${project_root}/scripts/setup_hive_it.py" \
  "${project_root}/tests/integration/setup.sql"

RAY_HIVE_HS2_HOST=127.0.0.1 \
RAY_HIVE_HS2_PORT="${RAY_HIVE_HS2_PORT:-10090}" \
  "${project_root}/.venv/bin/python" -m pytest "${project_root}/tests/integration" -v
