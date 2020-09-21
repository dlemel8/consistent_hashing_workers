FROM golang:1.15.2-alpine3.12

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG EXECUTABLE
RUN go build -o /app cmd/${EXECUTABLE}/main.go

CMD ["/app"]