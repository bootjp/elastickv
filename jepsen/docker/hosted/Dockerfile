ARG GO_VERSION
FROM eclipse-temurin:21-jre-jammy AS java
FROM golang:${GO_VERSION}-bookworm

# Jepsen's Bifurcan dependency requires Java 21 bytecode support.
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openssh-server sudo rsync curl netcat-openbsd iptables iproute2 \
      chrony libfaketime fuse3 procps psmisc openjdk-17-jre-headless leiningen \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd /root/.ssh /home/vagrant/.cache/go-build /home/vagrant/go \
    && chmod 700 /root/.ssh \
    && printf 'PermitRootLogin prohibit-password\nPasswordAuthentication no\n' > /etc/ssh/sshd_config.d/jepsen.conf

COPY entrypoint.sh /usr/local/bin/jepsen-entrypoint
RUN chmod +x /usr/local/bin/jepsen-entrypoint
WORKDIR /workspace/jepsen
ENV LEIN_ROOT=true
ENTRYPOINT ["/usr/local/bin/jepsen-entrypoint"]
CMD ["/usr/sbin/sshd", "-D", "-e"]
