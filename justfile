@flamegraph:
    sudo rm -rf data/index
    cargo flamegraph --root -- indexer local configs/indexer/local.toml

@worker:
    cargo run --release -- configs/webgraph/worker.toml

@master:
    cargo run --release -- configs/webgraph/master.toml

@frontend:
    cargo watch -x 'run -- frontend data/index'
