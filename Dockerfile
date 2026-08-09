FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY main.go schema.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o app .


FROM alpine:latest

RUN apk add git

WORKDIR /root/

COPY --from=builder /app/app .

CMD ["./app"]
