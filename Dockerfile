FROM golang:alpine AS builder
RUN apk add --no-cache make gcc vim musl-dev
WORKDIR /app
ADD go.mod .
ADD go.sum .
RUN go mod download
COPY . .
RUN make build
RUN apk update
RUN apk add python3
