FROM rust:latest as builder
WORKDIR /app
ARG DATABASE_URL
ENV DATABASE_URL=$DATABASE_URL
COPY . .
RUN cargo build --release

FROM rust:latest
WORKDIR /usr/local/bin
COPY --from=builder /app/target/release/rust-crud-api .
CMD ["./rust-crud-api"]