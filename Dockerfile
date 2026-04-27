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
# vite.config.ts targets `../../internal/admin/dist` relative to
# web/admin, but inside this stage we flatten the layout: emit
# straight to /spa/dist and copy it across to the Go build stage.
RUN npx vite build --outDir /spa/dist --emptyOutDir

FROM golang:latest AS build
WORKDIR $GOPATH/src/app
COPY . .
# Replace the placeholder dist/ with the real Vite bundle BEFORE the
# Go build so `//go:embed all:dist` picks up the SPA assets.
RUN rm -rf internal/admin/dist
COPY --from=spa-build /spa/dist internal/admin/dist
RUN CGO_ENABLED=0 go build -o /app .

FROM gcr.io/distroless/static:latest
COPY --from=build /app /app

CMD ["/app"]
