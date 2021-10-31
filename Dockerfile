FROM golang:1.17-alpine as builder

RUN apk add --no-cache git

WORKDIR /root

ARG version

COPY go.mod go.mod
COPY go.sum go.sum

RUN go mod download

COPY cmd cmd 
COPY internal internal
COPY model model

RUN echo $version

RUN go build -o cdn-pull -ldflags="-X 'github.com/livepeer/cdn-log-puller/model.Version=$version'"  cmd/cdn-pull/cdn-pull.go


FROM alpine
RUN apk add --no-cache ca-certificates

WORKDIR /root

COPY --from=builder /root/cdn-pull cdn-pull
