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

set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project_name="ray-hive-cluster-it"
compose_file="${project_root}/docker/ray/compose.yaml"
artifact_dir="${RAY_HIVE_CLUSTER_IT_LOG_DIR:-${project_root}/.artifacts/it/ray-cluster}"
runtime_facts_file="${artifact_dir}/runtime-facts.json"
runtime_image="ray-hive-cluster-runtime:${project_name}-$(date -u +%Y%m%d%H%M%S)-$$"
ray_base_image="ray-hive-ray-base:2.59.0"
ray_source_image="${RAY_HIVE_RAY_SOURCE_IMAGE:-docker.m.daocloud.io/rayproject/ray@sha256:0ffca209fb4c984d648db550b8dbe7879bdba3bbb5d96ee5894843cf6ac12eb5}"
hive_image="ray-hive-hive-base:4.2.0"
hive_source_image="${RAY_HIVE_HIVE_SOURCE_IMAGE:-docker.m.daocloud.io/apache/hive@sha256:b0d2842722567a89ac35da8af9002b6bba0fc2d0eb272664954a1fa46d694472}"
created_ray_tag=0
created_hive_tag=0

export RAY_HIVE_CLUSTER_RUNTIME_IMAGE="${runtime_image}"
export RAY_HIVE_IT_HIVE_IMAGE="${hive_image}"

mkdir -p "${artifact_dir}"
compose=(docker compose --project-name "${project_name}" --file "${compose_file}")

docker_clean_env=(
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY
  -u http_proxy -u https_proxy -u all_proxy
)

ensure_base_image() {
  local source_image="$1"
  local local_image="$2"
  local created_variable="$3"
  if "${docker_clean_env[@]}" docker image inspect "${local_image}" >/dev/null 2>&1; then
    return
  fi
  "${docker_clean_env[@]}" docker pull "${source_image}"
  "${docker_clean_env[@]}" docker tag "${source_image}" "${local_image}"
  printf -v "${created_variable}" '%s' 1
}

record_docker_state() {
  local suffix="$1"
  "${docker_clean_env[@]}" docker image ls --filter dangling=true --no-trunc \
    > "${artifact_dir}/dangling-images-${suffix}.log" 2>&1 || true
  "${docker_clean_env[@]}" docker image inspect \
    "${ray_base_image}" "${hive_image}" "${runtime_image}" \
    > "${artifact_dir}/images-${suffix}.log" 2>&1 || true
  "${docker_clean_env[@]}" docker system df \
    > "${artifact_dir}/docker-system-df-${suffix}.log" 2>&1 || true
}

cleanup() {
  local status=$?
  "${docker_clean_env[@]}" "${compose[@]}" logs --no-color \
    > "${artifact_dir}/compose.log" 2>&1 || true
  record_docker_state after-compose
  "${docker_clean_env[@]}" "${compose[@]}" down --volumes --remove-orphans || true
  "${docker_clean_env[@]}" docker image rm "${runtime_image}" >/dev/null 2>&1 || true
  if [[ "${created_ray_tag}" == "1" ]]; then
    "${docker_clean_env[@]}" docker image rm "${ray_base_image}" >/dev/null 2>&1 || true
  fi
  if [[ "${created_hive_tag}" == "1" ]]; then
    "${docker_clean_env[@]}" docker image rm "${hive_image}" >/dev/null 2>&1 || true
  fi
  record_docker_state after-cleanup
  exit "${status}"
}

trap cleanup EXIT

ensure_base_image "${ray_source_image}" "${ray_base_image}" created_ray_tag
ensure_base_image "${hive_source_image}" "${hive_image}" created_hive_tag
record_docker_state before

"${docker_clean_env[@]}" docker build --pull=false \
  --tag "${runtime_image}" \
  --build-arg "RAY_BASE_IMAGE=${ray_base_image}" \
  --file "${project_root}/docker/ray/Dockerfile" \
  "${project_root}"
"${docker_clean_env[@]}" "${compose[@]}" up --detach --wait --no-build

"${docker_clean_env[@]}" "${compose[@]}" exec -T runner \
  python scripts/release_evidence.py capture-runtime \
  --hive-version 4.2.0 --output - \
  > "${runtime_facts_file}"

"${docker_clean_env[@]}" "${compose[@]}" exec -T runner \
  python scripts/setup_hive_it.py tests/integration/setup.sql \
  2>&1 | tee "${artifact_dir}/setup.log"

runtime_env_json='{"env_vars":{"RAY_HIVE_HS2_HOST":"hiveserver2","RAY_HIVE_HS2_PORT":"10000","RAY_HIVE_RAY_ADDRESS":"10.252.0.10:6379","RAY_HIVE_MIN_NODES":"3","PYTHONPATH":"./src:./tests","RAY_USAGE_STATS_ENABLED":"0"}}'
set -o pipefail
"${docker_clean_env[@]}" "${compose[@]}" exec -T runner ray job submit \
  --address=http://10.252.0.10:8265 \
  --working-dir=/workspace/ray-hive \
  --runtime-env-json="${runtime_env_json}" \
  -- python -m pytest tests/integration/test_ray_cluster.py -m cluster -vv \
  2>&1 | tee "${artifact_dir}/pytest.log"
