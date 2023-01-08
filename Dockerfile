FROM rust:bullseye AS builder

WORKDIR /cuely

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

RUN rustup toolchain install beta && rustup default beta && rustup component add rustfmt

COPY . .

RUN cd frontend && npm install && npm run build

RUN ORT_STRATEGY=system ORT_LIB_LOCATION=/usr/lib/libonnxruntime.so.1.13.1 cargo build --release \
    && mkdir /cuely/bin \
    && find target/release -maxdepth 1 -perm /a+x -type f -exec mv {} /cuely/bin \;

FROM debian:bullseye-slim AS cuely

LABEL org.opencontainers.image.title="Cuely"
LABEL maintainer="Cuely <hello@cuely.io>"
LABEL org.opencontainers.image.licenses="AGPL-3.0"

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
    libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /cuely

COPY --from=builder /cuely/bin/cuely /usr/local/bin/cuely
COPY --from=builder /cuely/frontend/dist frontend/dist
COPY --from=builder /usr/lib/libonnxruntime.so* /usr/lib

ENTRYPOINT ["/usr/local/bin/cuely"]
