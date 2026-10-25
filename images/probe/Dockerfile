FROM scratch
COPY lab-probe /lab-probe
COPY --chown=65532:65532 ledger/ /ledger/
LABEL agent-runtime-lab.managed="true"
USER 65532:65532
ENTRYPOINT ["/lab-probe"]
CMD ["noop"]
