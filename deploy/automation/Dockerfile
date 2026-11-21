# syntax=docker/dockerfile:1
FROM golang:1.27-bookworm AS tools
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /raftadmin ./cmd/raftadmin

FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/*
ADD --checksum=sha256:bb766f710eef8ede859c18578c72c327597cd4c8a85b06001b1f3843c6019386 https://github.com/cli/cli/releases/download/v2.102.0/gh_2.102.0_linux_amd64.tar.gz /tmp/gh.tar.gz
RUN tar -xzf /tmp/gh.tar.gz --strip-components=2 -C /usr/local/bin gh_2.102.0_linux_amd64/bin/gh && rm /tmp/gh.tar.gz
COPY deploy/automation/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --from=tools /raftadmin /usr/local/bin/raftadmin
COPY deploy/automation/agent.py deploy/automation/compatibility.py \
     deploy/automation/compatibility_profile.py deploy/automation/compatibility_runtime.py \
     deploy/automation/compatibility_validation.py deploy/automation/settings.py deploy/automation/source_review.py \
     deploy/automation/human_approval.py /app/
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 65532:65532
WORKDIR /app
ENTRYPOINT ["python3", "/app/agent.py"]
