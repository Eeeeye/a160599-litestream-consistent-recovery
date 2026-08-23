FROM golang:1.25.8-bookworm

WORKDIR /app

ENV GOTOOLCHAIN=local

COPY project/go.mod project/go.sum ./
RUN go mod download

COPY project/ /app/
RUN mkdir -p /app/bin /logs/verifier \
    && go build -o /app/bin/litestream ./cmd/litestream

ENV GOPROXY=off
ENV GOSUMDB=off

USER root
