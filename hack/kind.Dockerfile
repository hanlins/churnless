FROM scratch

COPY bin/manager-kind /manager

USER 65532:65532
ENTRYPOINT ["/manager"]
