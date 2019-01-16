FROM golang:1.11 as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build/
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o aws-node-cleanup .

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/aws-node-cleanup /aws-node-cleanup

WORKDIR /
ENTRYPOINT ["/aws-node-cleanup"]
