FROM golang:1.22.10-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o main .

# EXPOSE 1234/udp

CMD ["./main"]
