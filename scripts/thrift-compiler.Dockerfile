FROM cspwizard/thrift@sha256:526c07ac0e08d86b35353a3f474558b6719735227bf7147f6df0f64ab58df04c

RUN thrift --version | grep -F "Thrift version 0.22.0"

ENTRYPOINT ["thrift"]
