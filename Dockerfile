# build builder
FROM golang:1.12-alpine as builder

ARG CODEPATH

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories \
	&& apk update \
	&& apk add git \
	&& rm -rf /var/cache/apk/*

WORKDIR $GOPATH/src/$CODEPATH

COPY . .

RUN GO111MODULE=on CGO_ENABLED=0 GOOS=linux GOPROXY=https://mirrors.aliyun.com/goproxy/ go build -o /app/app main.go

# build server
FROM alpine:3.8

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /app

COPY --from=builder /app/app .
RUN chmod +x /app/app

ENTRYPOINT ["/app/app"]

