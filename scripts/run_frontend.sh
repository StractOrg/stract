#!/bin/bash
trap 'kill $(jobs -p)' EXIT
cargo run --release -- search-server configs/search_server.toml &
cargo run --release -- frontend configs/frontend.toml
