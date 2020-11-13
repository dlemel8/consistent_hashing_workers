FROM golang:1.15.2-alpine3.12

RUN apk update && apk add gcc libzmq musl-dev pkgconfig zeromq-dev

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG EXECUTABLE
RUN go build -o /app cmd/${EXECUTABLE}/main.go

CMD ["/app"]