# Build and publish the root Dockerfile image first, then supply its digest.
ARG ELASTICKV_IMAGE=ghcr.io/bootjp/elastickv@sha256:e8e05151a6d824730b177d8c3e76006737f953bc8b11d44d405a3b0605d0e688
FROM ${ELASTICKV_IMAGE} AS server

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates fuse3 util-linux \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid 65532 elastickv \
    && useradd --no-log-init --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin elastickv
COPY --from=server /app /app
USER 65532:65532
CMD ["/app"]
