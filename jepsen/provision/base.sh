#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:-db}"
PUBKEY="${2:-}"

echo "[jepsen] provisioning role=${ROLE}"
sudo apt-get update -y
sudo apt-get install -y --no-install-recommends \
  curl netcat-openbsd rsync iptables chrony libfaketime \
  openjdk-17-jre-headless leiningen git build-essential jq

sudo systemctl enable --now chrony

# Append Jepsen nodes to /etc/hosts if not already present
if ! grep -q "192.168.56.10 ctrl" /etc/hosts; then
cat <<'EOF' | sudo tee -a /etc/hosts >/dev/null

# Jepsen Cluster
192.168.56.10 ctrl
192.168.56.11 n1
192.168.56.12 n2
192.168.56.13 n3
192.168.56.14 n4
192.168.56.15 n5
EOF
fi

install -d -m700 /home/vagrant/.ssh
if [ "$ROLE" = "ctrl" ]; then
  GO_VERSION=1.25.5
  ARCH=$(dpkg --print-architecture)
  if [ "$ARCH" = "arm64" ]; then
    GO_ARCH="arm64"
  else
    GO_ARCH="amd64"
  fi

  if ! command -v go >/dev/null 2>&1 || [[ "$(go version | awk '{print $3}')" != "go${GO_VERSION}" ]]; then
    echo "[jepsen] installing go ${GO_VERSION} for ${GO_ARCH}"
    curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" -o /tmp/go.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go.tar.gz
  fi
  echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' | sudo tee /etc/profile.d/go.sh >/dev/null

  if [ ! -f /home/vagrant/.ssh/id_rsa ]; then
    if [ -f /home/vagrant/elastickv/jepsen/.ssh/ctrl_id_rsa ]; then
      cp /home/vagrant/elastickv/jepsen/.ssh/ctrl_id_rsa /home/vagrant/.ssh/id_rsa
    else
      if ! command -v ssh-keygen >/dev/null 2>&1; then
        sudo apt-get install -y --no-install-recommends openssh-client
      fi
      ssh-keygen -t rsa -b 2048 -N "" -f /home/vagrant/.ssh/id_rsa
    fi
    chmod 600 /home/vagrant/.ssh/id_rsa
    chown vagrant:vagrant /home/vagrant/.ssh/id_rsa
  fi
  if [ -z "${PUBKEY}" ] && [ -f /home/vagrant/.ssh/id_rsa.pub ]; then
    PUBKEY="$(cat /home/vagrant/.ssh/id_rsa.pub)"
  fi
  cat <<'EOF' > /home/vagrant/.ssh/config
Host n1 n2 n3 n4 n5
  User vagrant
  IdentityFile /home/vagrant/.ssh/id_rsa
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel QUIET
EOF
  chown vagrant:vagrant /home/vagrant/.ssh/config
fi

touch /home/vagrant/.ssh/authorized_keys
if [ -n "${PUBKEY}" ]; then
  if ! grep -Fq "${PUBKEY}" /home/vagrant/.ssh/authorized_keys; then
    echo "${PUBKEY}" >> /home/vagrant/.ssh/authorized_keys
  fi
fi
chown vagrant:vagrant /home/vagrant/.ssh/authorized_keys
chmod 600 /home/vagrant/.ssh/authorized_keys

sudo mkdir -p /opt/elastickv/bin /var/lib/elastickv
