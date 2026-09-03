# syntax=docker/dockerfile:1
#
# open-pubusb container image (T048, specs/001-local-pubsub-service/tasks.md).
#
# Design (see specs/001-local-pubsub-service/research.md, "R6. Static binary
# and container image"):
#   - cargo-chef for a dependency-only build layer (rebuilds only when
#     Cargo.toml/Cargo.lock change, not on every source edit).
#   - cargo-zigbuild (https://github.com/rust-cross/cargo-zigbuild) + zig
#     (installed via the `ziglang` PyPI wheel, which ships a `zig` shim - no
#     manual tarball download needed) to cross-link musl static binaries.
#     The builder stage always runs on `--platform=$BUILDPLATFORM` (native,
#     never under QEMU) and cross-compiles to `$TARGETARCH` purely via zig's
#     bundled cross-linker/libc, per research.md's guidance - only the final
#     copy stage is multi-arch. `TARGETARCH` (amd64/arm64, set automatically
#     by `docker buildx build --platform=...`) is mapped to the matching
#     musl target triple below.
#     Only x86_64-unknown-linux-musl is verified, and it is the only
#     platform .github/workflows/release.yml publishes - that job builds
#     `linux/amd64` alone, so the image pushed to GHCR is single-arch. The
#     arm64 mapping below is kept so that a local `docker buildx build
#     --platform linux/arm64` still works, but it has never been run
#     against real aarch64 hardware - treat it as "should work per
#     cargo-zigbuild's documented cross-compilation support", not "proven".
#   - Final stage: gcr.io/distroless/static-debian12:nonroot (no shell, no
#     libc even - matches the musl static binary; includes /etc/passwd,
#     CA certs and tzdata, and a non-root `nonroot` user/group).
#
# Repo constraint: crates/open-pubusb-proto's build.rs compiles
# third_party/googleapis/**/*.proto with `protox` (pure Rust - no system
# `protoc` needed), so the only requirement is that the submodule content is
# present in the build context. This repo's .dockerignore deliberately does
# NOT exclude third_party/, so `git submodule update --init` (already run
# for this repo) is sufficient - just make sure it has been run before
# `docker build .`, since Docker does not understand git submodules itself.

ARG RUST_IMAGE=lukemathwalker/cargo-chef:latest-rust-1-bookworm

FROM --platform=$BUILDPLATFORM ${RUST_IMAGE} AS chef
WORKDIR /build

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG TARGETARCH
# Map buildx's TARGETARCH (amd64/arm64) to the matching musl target triple.
RUN case "${TARGETARCH}" in \
      amd64) echo x86_64-unknown-linux-musl > /target-triple ;; \
      arm64) echo aarch64-unknown-linux-musl > /target-triple ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac
# zig (via the ziglang PyPI wheel) + cargo-zigbuild give us a musl
# cross-linker without needing a separate musl-gcc/musl-tools apt package.
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --break-system-packages --no-cache-dir ziglang \
    && cargo install cargo-zigbuild --locked
# Resolve/install the toolchain pinned by rust-toolchain.toml *before* adding
# the musl target: if the target is added against the chef base image's
# default toolchain and rust-toolchain.toml (copied in below) later causes
# rustup to install/switch to a different toolchain instance to satisfy its
# `components = [...]` list, the earlier `target add` is silently lost and
# the final `cargo zigbuild` fails with "can't find crate for std".
COPY rust-toolchain.toml rust-toolchain.toml
RUN rustup show \
    && rustup target add "$(cat /target-triple)"

COPY --from=planner /build/recipe.json recipe.json
# Dependency-only build, cached as long as recipe.json (derived from
# Cargo.toml/Cargo.lock) is unchanged. `--zigbuild` tells cargo-chef to
# invoke `cargo zigbuild` instead of plain `cargo build` for the musl target.
RUN cargo chef cook --release --zigbuild --target "$(cat /target-triple)" --recipe-path recipe.json

COPY . .
RUN target="$(cat /target-triple)" \
    && cargo zigbuild --release --target "${target}" -p open-pubusb \
    && mkdir -p /out \
    && cp "target/${target}/release/open-pubusb" /out/open-pubusb

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=builder /out/open-pubusb /usr/local/bin/open-pubusb

USER nonroot
ENV OPEN_PUBUSB__SERVER__LISTEN=0.0.0.0:8085 \
    OPEN_PUBUSB__SERVER__ADMIN_LISTEN=0.0.0.0:8086 \
    OPEN_PUBUSB__STORAGE__DATA_DIR=/data

VOLUME /data
EXPOSE 8085 8086
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=3s CMD ["/usr/local/bin/open-pubusb", "health", "--url", "http://127.0.0.1:8086/readyz"]

ENTRYPOINT ["/usr/local/bin/open-pubusb"]
CMD ["serve"]
