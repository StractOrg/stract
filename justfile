@flamegraph:
    sudo rm -rf data/index
    cargo flamegraph --root -- indexer local configs/indexer/local.toml

@worker:
    cargo run --release -- configs/webgraph/worker.toml

@master:
    cargo run --release -- configs/webgraph/master.toml

@frontend:
    cargo watch -x 'run -- frontend data/index queries_us.csv'

@local:
    rm -rf data/index/
    cargo build --release
    ./target/release/cuely indexer local configs/indexer/local.toml
    mv data/index/CC-MAIN-20220116093137-20220116123137-00049.warc.gz/* data/index/
    ./target/release/cuely frontend data/index/ queries_us.csv