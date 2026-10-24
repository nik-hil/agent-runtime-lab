FROM scratch
COPY lab-probe /lab-probe
LABEL agent-runtime-lab.managed="true"
USER 65532:65532
ENTRYPOINT ["/lab-probe"]
CMD ["noop"]
