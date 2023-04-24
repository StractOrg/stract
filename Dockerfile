FROM rust:bullseye AS builder

WORKDIR /stract

RUN echo "Adding Node.js PPA" \
    && curl -s https://deb.nodesource.com/setup_18.x | bash

RUN apt-get update -y \
    && apt-get -y install ca-certificates \
    clang \
    libssl-dev \
    nodejs

RUN wget https://github.com/microsoft/onnxruntime/releases/download/v1.13.1/onnxruntime-linux-x64-1.13.1.tgz -P /tmp \
    && tar -xf /tmp/onnxruntime-linux-x64-1.13.1.tgz -C /tmp \ 
    && mv /tmp/onnxruntime-linux-x64-1.13.1/lib/* /usr/lib

RUN rustup component add rustfmt

COPY . .

RUN cd frontend && npm install && npm run build

RUN ORT_STRATEGY=system ORT_LIB_LOCATION=/usr/lib/libonnxruntime.so.1.13.1 cargo build --release \
    && mkdir /stract/bin \
    && find target/release -maxdepth 1 -perm /a+x -type f -exec mv {} /stract/bin \;

FROM debian:bullseye-slim AS stract

LABEL org.opencontainers.image.title="Stract"
LABEL maintainer="Stract <hello@trystract.com>"
LABEL org.opencontainers.image.licenses="AGPL-3.0"

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
    libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /stract

COPY --from=builder /stract/bin/stract /usr/local/bin/stract
COPY --from=builder /stract/frontend/dist frontend/dist
COPY --from=builder /usr/lib/libonnxruntime.so.1.13.1 /usr/lib
COPY --from=builder /usr/lib/libonnxruntime.so /usr/lib

ENTRYPOINT ["/usr/local/bin/stract"]
