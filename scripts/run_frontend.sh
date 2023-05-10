#!/bin/bash
trap 'kill $(jobs -p)' EXIT
cargo run $1 -- frontend configs/frontend.toml &
cargo run $1 -- search-server configs/search_server.toml &
cargo run $1 -- webgraph server configs/webgraph/server.toml
