#!/usr/bin/env bash
set -euo pipefail

# Copy source to writable area
mkdir -p /root/elastickv
rsync -a /jepsen-ro/ /root/elastickv/ --exclude .git --exclude jepsen/target --exclude jepsen/tmp-home

cd /root/elastickv/jepsen

# Install Go
if ! command -v go >/dev/null 2>&1; then
    TOOLCHAIN="$(awk '$1 == "toolchain" { print $2 }' ../go.mod 2>/dev/null | head -n1 || true)"
    if [ -n "${TOOLCHAIN}" ]; then
        GO_VERSION="${TOOLCHAIN#go}"
    else
        GO_VERSION=1.26.0
    fi
    ARCH="amd64" # Assuming amd64 for now, or detect
    if [ "$(uname -m)" = "aarch64" ]; then ARCH="arm64"; fi

    TARBALL="go${GO_VERSION}.linux-${ARCH}.tar.gz"
    curl -fsSL "https://go.dev/dl/${TARBALL}" -o "${TARBALL}"
    curl -fsSL "https://go.dev/dl/${TARBALL}.sha256" -o "${TARBALL}.sha256"

    expected_sha256="$(awk '{print $1}' < "${TARBALL}.sha256")"
    actual_sha256="$(sha256sum "${TARBALL}" | awk '{print $1}')"
    if [ "${expected_sha256}" != "${actual_sha256}" ]; then
        echo "Go toolchain checksum mismatch for ${TARBALL}" >&2
        exit 1
    fi

    tar -C /usr/local -xzf "${TARBALL}"
    rm -f "${TARBALL}" "${TARBALL}.sha256"
    export PATH=$PATH:/usr/local/go/bin
fi

# Install Leiningen
if ! command -v lein >/dev/null 2>&1; then
    apt-get update -y
    apt-get install -y --no-install-recommends leiningen
fi

# Generate or install SSH key for control node to connect to others
if [ ! -f /root/.ssh/id_rsa ]; then
    mkdir -p /root/.ssh
    if [ -n "${JEPSEN_SSH_PRIVATE_KEY:-}" ]; then
        printf "%s" "${JEPSEN_SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa
    elif [ -n "${JEPSEN_SSH_PRIVATE_KEY_PATH:-}" ] && [ -f "${JEPSEN_SSH_PRIVATE_KEY_PATH}" ]; then
        cp "${JEPSEN_SSH_PRIVATE_KEY_PATH}" /root/.ssh/id_rsa
    elif [ -f /jepsen-ro/jepsen/docker/id_rsa ]; then
        # Backward-compatible path (local, uncommitted key file)
        cp /jepsen-ro/jepsen/docker/id_rsa /root/.ssh/id_rsa
    else
        if ! command -v ssh-keygen >/dev/null 2>&1; then
            apt-get update -y
            apt-get install -y --no-install-recommends openssh-client
        fi
        ssh-keygen -t rsa -b 2048 -N "" -f /root/.ssh/id_rsa
    fi
    chmod 600 /root/.ssh/id_rsa
    # Disable strict host checking
    echo "Host *" > /root/.ssh/config
    echo "  StrictHostKeyChecking no" >> /root/.ssh/config
    echo "  UserKnownHostsFile /dev/null" >> /root/.ssh/config
    echo "  User vagrant" >> /root/.ssh/config
fi

# Run test
# Nodes are reachable by hostname (n1, n2...) in docker network
export LEIN_ROOT=true
lein run -m elastickv.redis-workload \
  --nodes n1,n2,n3,n4,n5 \
  --time-limit 60 \
  --rate 10 \
  --faults partition,kill,clock \
  --concurrency 10
