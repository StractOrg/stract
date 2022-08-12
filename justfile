@flamegraph:
    sudo rm -rf data/index
    cargo flamegraph --root -- indexer local configs/indexer/local.toml

@worker:
    cargo run --release -- configs/webgraph/worker.toml

@master:
    cargo run --release -- configs/webgraph/master.toml

@frontend:
    cargo watch -x 'run -- frontend data/index queries_us.csv data/entity'

@local:
    rm -rf data/index/
    cargo run --release -- indexer local configs/indexer/local.toml
    mv data/index/CC-MAIN-20220116093137-20220116123137-00049.warc.gz/* data/index/
    just frontend

@entity:
    rm -rf data/entity
    cargo run --release -- indexer entity data/enwiki-20220801-pages-articles-multistream.xml.bz2 data/entity
