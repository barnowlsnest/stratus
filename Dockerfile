FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN GOPRIVATE=github.com/barnowlsnest go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o ./dist/ ./cmd/stratus.go

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /usr/wal
WORKDIR /app
COPY --from=builder ./src/dist/stratus .
USER nonroot:nonroot
EXPOSE 8000
ENTRYPOINT ["/app/stratus"]
