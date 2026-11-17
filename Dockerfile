# 多阶段构建：纯 Go SQLite 驱动，无需 cgo
FROM golang:1.26.11-alpine@sha256:ecb67f987aa1a57794c91d19ade7ae0209f2f0f5496aaef9e6ae85ddf92d02ab AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /out/litepay .

FROM alpine:3.22@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 litepay \
    && adduser -S -D -H -u 10001 -G litepay litepay \
    && mkdir -p /app/data \
    && chown -R litepay:litepay /app
ENV TZ=Asia/Shanghai
WORKDIR /app
COPY --from=builder --chown=litepay:litepay --chmod=0755 /out/litepay /app/litepay
COPY --chown=litepay:litepay --chmod=0400 config.production.yaml /app/config.yaml
VOLUME ["/app/data"]
EXPOSE 8080
USER 10001:10001
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8080/healthz"]
ENTRYPOINT ["/app/litepay"]
CMD ["-config", "/app/config.yaml"]
