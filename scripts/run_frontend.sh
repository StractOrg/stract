#!/bin/bash
trap 'kill $(jobs -p)' EXIT
cargo run -- search-server configs/search_server.toml &
cargo run -- frontend configs/frontend.toml
