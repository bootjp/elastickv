#!/usr/bin/env bash
set -euo pipefail
ssh-keygen -A
install -m 600 /run/jepsen-ssh/id_ed25519.pub /root/.ssh/authorized_keys
if [[ -e /workspace/.git ]]; then
  # Checkout belongs to the runner UID; the control container runs as root.
  git config --global --add safe.directory /workspace
  git -C /workspace status --porcelain >/dev/null
fi
exec "$@"
