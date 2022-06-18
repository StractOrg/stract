@flamegraph:
    sudo rm -rf webgraph
    cargo flamegraph --root -- configs/webgraph/local/profile.toml

@worker:
    cargo run --release -- configs/webgraph/worker.toml

@master:
    cargo run --release -- configs/webgraph/master.toml
