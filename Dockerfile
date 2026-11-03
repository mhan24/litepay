# 多阶段构建：纯 Go SQLite 驱动，无需 cgo
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /out/litepay .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 litepay \
    && adduser -S -D -H -u 10001 -G litepay litepay \
    && mkdir -p /app/data \
    && chown -R litepay:litepay /app
ENV TZ=Asia/Shanghai
WORKDIR /app
COPY --from=builder --chown=litepay:litepay --chmod=0755 /out/litepay /app/litepay
COPY --chown=litepay:litepay --chmod=0400 config.example.yaml /app/config.yaml
VOLUME ["/app/data"]
EXPOSE 8080
USER 10001:10001
ENTRYPOINT ["/bin/sh", "-c", "umask 077; exec /app/litepay -config /app/config.yaml"]
