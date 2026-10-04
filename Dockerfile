# syntax=docker/dockerfile:1

# ---- build: static binaries for the server and the burst tool ----
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/burst ./cmd/burst

# ---- burst: `docker build --target burst -t seat-burst . && docker run --rm seat-burst <BASE_URL>` ----
FROM gcr.io/distroless/static-debian12:nonroot AS burst
COPY --from=build /out/burst /burst
ENTRYPOINT ["/burst"]

# ---- runtime (default, last stage): what Railway and docker compose run ----
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /out/server /server
ENV PORT=8080 \
    APP_ENV=production
EXPOSE 8080
USER nonroot:nonroot
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/server", "healthcheck"]
ENTRYPOINT ["/server"]
