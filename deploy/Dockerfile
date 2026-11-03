# 运行镜像：只拷贝已交叉编译好的静态二进制（CGO_ENABLED=0）
# 服务器为 linux/arm64，构建机上用 GOOS=linux GOARCH=arm64 编译后上传
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 litepay \
    && adduser -S -D -H -u 10001 -G litepay litepay \
    && mkdir -p /app/data \
    && chown -R litepay:litepay /app
ENV TZ=Asia/Shanghai

WORKDIR /app
COPY --chown=litepay:litepay --chmod=0755 litepay /app/litepay

USER 10001:10001
EXPOSE 3000
ENTRYPOINT ["/bin/sh", "-c", "umask 077; exec /app/litepay -config /app/config.yaml"]
