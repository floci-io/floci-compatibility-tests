FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev pkgconf openssl-dev
WORKDIR /app
COPY Cargo.toml .
# pre-fetch dependencies
RUN mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/floci-test /floci-test

ENV FLOCI_ENDPOINT=http://floci:4566
ENTRYPOINT ["/floci-test"]
