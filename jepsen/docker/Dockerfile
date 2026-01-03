FROM debian:bookworm

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
# - openssh-server: for Jepsen control
# - sudo: for Jepsen control
# - iptables: for network partitioning (requires --privileged)
# - iproute2: for network introspection
# - netcat-openbsd: for port checking
# - curl, wget: utilities
# - libfaketime: for clock skew nemesis
# - rsync: for file transfer
# - systemd-sysv: minimal init system (optional, but good for compatibility)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-server \
    sudo \
    iptables \
    iproute2 \
    netcat-openbsd \
    curl \
    wget \
    rsync \
    libfaketime \
    less \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Setup SSH
RUN mkdir /var/run/sshd
# Allow root login with password (for simplicity in test env)
#RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# Create vagrant user
RUN useradd -m -s /bin/bash vagrant && \
    echo "vagrant:vagrant" | chpasswd && \
    usermod -aG sudo vagrant

# Allow sudo without password
RUN echo "vagrant ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/vagrant

# Add Vagrant insecure public key
RUN mkdir -p /home/vagrant/.ssh && \
    chmod 700 /home/vagrant/.ssh
COPY vagrant.pub /home/vagrant/.ssh/authorized_keys
RUN chmod 600 /home/vagrant/.ssh/authorized_keys && \
    chown -R vagrant:vagrant /home/vagrant/.ssh

# Expose SSH
EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]
