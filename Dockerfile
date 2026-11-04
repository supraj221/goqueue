# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/goqueued ./cmd/goqueued \
 && go build -trimpath -ldflags="-s -w" -o /out/goqueue-bench ./cmd/goqueue-bench \
 && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/goqueued /out/goqueue-bench /usr/local/bin/
COPY --from=build --chown=65532:65532 /data /data
VOLUME /data
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/goqueued"]
