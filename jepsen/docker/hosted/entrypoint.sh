#!/usr/bin/env bash
set -euo pipefail
ssh-keygen -A
install -m 600 /run/jepsen-ssh/id_ed25519.pub /root/.ssh/authorized_keys
exec "$@"
