# Vertica 25.4 is the last release that accepts the free Community Edition license
# (26.1+ refuses it); the multiarch tag runs natively on arm64 and amd64.
FROM opentext/vertica-k8s:25.4.0-0-multiarch
USER root
RUN mkdir -p /data && chown 998:996 /data
COPY --chown=998:996 --chmod=755 entrypoint.sh /usr/local/bin/vertica-entrypoint.sh
# The image has no passwd entry for uid 998; vcluster and the cert script read $USER.
ENV USER=dbadmin HOME=/data/home VTMPDIR=/tmp/vtmp
USER 998:996
ENTRYPOINT ["/usr/local/bin/vertica-entrypoint.sh"]
