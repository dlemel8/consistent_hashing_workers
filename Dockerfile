FROM golang:1.15.2-alpine3.12

ARG EXECUTABLE

WORKDIR /go/src/app
COPY . .

RUN go build -o /app cmd/${EXECUTABLE}/main.go

RUN ls -lh /app
CMD ["/app"]