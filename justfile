@unpack-data:
    rm -rf data
    tar -zxvf data.tar.gz

@flamegraph:
    sudo rm -rf data/index
    cargo flamegraph --root -- indexer local configs/indexer/local.toml

@worker:
    cargo run --release -- webgraph worker configs/webgraph/worker.toml

@master:
    cargo run --release -- webgraph master configs/webgraph/master.toml

@webgraph:
    rm -rf data/webgraph
    cargo run --release -- webgraph local configs/webgraph/local.toml

@frontend:
    cargo watch -x 'run -- frontend data/index data/queries_us.csv data/entity data/bangs.json'

@local:
    rm -rf data/index/
    cargo run --release -- indexer local configs/indexer/local.toml
    mv data/index/CC-MAIN-*/* data/index/
    just frontend

@entity:
    rm -rf data/entity
    cargo run --release -- indexer entity data/enwiki-20220801-pages-articles-multistream.xml.bz2 data/entity

@pack-data:
    rm -f data.tar.gz
    tar --exclude="data/enwiki*" --exclude="data/warc_files" --exclude="data/webgraph" -zcvf  data.tar.gz data
