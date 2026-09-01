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

ARG RAY_BASE_IMAGE=ray-hive-ray-base:2.56.1
FROM ${RAY_BASE_IMAGE}

WORKDIR /workspace/ray-hive
COPY pyproject.toml README.md LICENSE NOTICE ./
COPY src ./src
COPY tests ./tests
COPY scripts ./scripts

RUN python -m pip install --no-cache-dir \
      "hatchling>=1.27,<2" \
      "pyarrow==19.0.1" \
      "pytest>=8.3,<10" \
      "pytest-timeout>=2.3,<3" \
      "thrift==0.22.0" \
      "typing-extensions>=4.12,<5" \
    && python -m pip install --no-cache-dir --no-deps -e . \
    && python -c "import platform; from importlib.metadata import version; actual = {'python': platform.python_version(), 'ray': version('ray'), 'pyarrow': version('pyarrow'), 'thrift': version('thrift')}; expected = {'ray': '2.56.1', 'pyarrow': '19.0.1', 'thrift': '0.22.0'}; print('ray-hive cluster runtime:', actual); assert actual['python'].split('.')[:2] == ['3', '12']; assert {name: actual[name] for name in expected} == expected"
