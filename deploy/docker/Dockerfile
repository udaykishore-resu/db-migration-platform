# Multi-stage build producing a distroless image per service.
#
# Build with:  docker build -f deploy/docker/Dockerfile --build-arg SERVICE=cdc-applier -t db-migration/cdc-applier .

FROM golang:1.24-alpine AS build
ARG SERVICE=cdc-applier
ARG VERSION=dev

WORKDIR /src

# Dependencies first, so a source change does not invalidate the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off: the resulting binary is fully static, which is what allows the
# distroless base below to contain nothing but the binary itself.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/service"]
