FROM golang:1.22 as build
WORKDIR /stbb
COPY . .
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/root/.cache/go-build,id=gobuild \
    --mount=type=cache,target=/go/pkg/mod,id=gopkg \
    go install

FROM alpine
COPY --from=build /go/bin/stbb /usr/local/bin/stbb
