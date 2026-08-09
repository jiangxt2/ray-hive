FROM ubuntu:22.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends thrift-compiler \
    && thrift --version | grep -F "Thrift version 0.16.0" \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["thrift"]
