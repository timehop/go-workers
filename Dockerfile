FROM golang:1.22

WORKDIR /go/src/go-workers

# Install dlv for optional debugging
RUN go install github.com/go-delve/delve/cmd/dlv@latest

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

# Build test binary with debug symbols
RUN go test -c -gcflags "all=-N -l" -o go-workers.test

CMD ["go", "test", "-v", "-race", "./..."]