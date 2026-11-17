FROM golang:1.26-bookworm AS tools
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /raftadmin ./cmd/raftadmin

FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/*
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
