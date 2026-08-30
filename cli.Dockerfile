FROM golang:1.26 AS builder

# Pinned so the image build is reproducible; bump with --build-arg or here.
ARG TASK_VERSION=v3.53.1
ARG GOLANGCI_LINT_VERSION=v2.13.2

RUN go install github.com/go-task/task/v3/cmd/task@${TASK_VERSION} && \
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}

WORKDIR /src
COPY go.mod go.sum ./
RUN GOPRIVATE=github.com/barnowlsnest go mod download

COPY . .

# go-build-cli runs sanity (fmt, vet, lint, test) first, then builds with
# -trimpath -ldflags="-s -w".
RUN CGO_ENABLED=0 GOOS=linux task go-build-cli

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /src/dist/cli/stratuscli .

# The TUI needs a terminal type to pick its color profile; override with -e TERM.
ENV TERM=xterm-256color

USER nonroot:nonroot
ENTRYPOINT ["/app/stratuscli"]
