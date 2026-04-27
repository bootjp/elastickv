# Build the admin SPA bundle first so the Go build stage's
# `//go:embed all:dist` (internal/admin/embed.go) picks up the real
# Vite output instead of the "(bundle missing)" placeholder. The
# placeholder index.html is committed for the embed pragma to find
# during local `go build`/test runs that haven't run npm; the real
# bundle replaces it inside the image.
FROM node:22-alpine AS spa-build
WORKDIR /spa
# Bring just the package manifests first so the npm cache survives
# unrelated source changes; the layer rebuilds only when deps shift.
COPY web/admin/package.json web/admin/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/admin/ ./
# Use `npm run build` (not `npx vite build`) so the package.json
# script's `tsc -b && vite build` chain runs in full -- a TypeScript
# error that would fail a local build must also fail the image build,
# not silently slip through. vite.config.ts already sets
# emptyOutDir=true, so only --outDir is forwarded here.
RUN npm run build -- --outDir /spa/dist

FROM golang:1.25 AS build
WORKDIR $GOPATH/src/app
COPY . .
# Replace the placeholder dist/ with the real Vite bundle BEFORE the
# Go build so `//go:embed all:dist` picks up the SPA assets.
RUN rm -rf internal/admin/dist
COPY --from=spa-build /spa/dist internal/admin/dist
RUN CGO_ENABLED=0 go build -o /app .

# Pinned to a multi-arch index digest of `gcr.io/distroless/static:nonroot`
# captured 2026-04-28. The `:latest` (or even `:nonroot`) tag floats with
# upstream rebuilds; pinning a digest makes intermediate-image
# reproducibility match the explicit golang:1.25 / node:22-alpine pins
# above and prevents a silent base-image swap from changing the image
# layout under us.
#
# `:nonroot` resolves to the same OS layer as `:latest` but with `USER
# nonroot` baked in (UID 65532). Combined with the explicit USER line
# below (defence-in-depth — the upstream USER directive could change),
# the Go binary runs unprivileged inside the container, addressing
# Trivy DS-0002 (root container) without changing what the binary
# itself does. The Go binary is statically linked CGO_ENABLED=0 and
# does not bind privileged ports, so unprivileged execution has no
# functional cost.
#
# Operational note: bind-mounted secrets on the host (e.g.
# /etc/elastickv/admin-hs256.b64) must be readable by UID 65532 inside
# the container. Either chown them to 65532:65532 or relax permissions
# to mode 0444 on the host. The host's bootjp group is no longer
# sufficient because UID 65532 is in no host group by default.
FROM gcr.io/distroless/static@sha256:e3f945647ffb95b5839c07038d64f9811adf17308b9121d8a2b87b6a22a80a39
USER nonroot:nonroot
COPY --from=build /app /app

CMD ["/app"]
