@unpack-data:
    rm -rf data
    tar -zxvf data.tar.gz

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
    wget -nc -P data/warc_files https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/segments/1642320299852.23/warc/CC-MAIN-20220116093137-20220116123137-00049.warc.gz
    rm -rf data/index/
    RUSTFLAGS="-C target-cpu=native" cargo run --release -- indexer local configs/indexer/local.toml
    mv data/index/CC-MAIN-*/* data/index/
    just frontend

@entity:
    rm -rf data/entity
    cargo run --release -- indexer entity data/enwiki-20220801-pages-articles-multistream.xml.bz2 data/entity

@pack-data:
    rm -f data.tar.gz
    tar --exclude="data/enwiki*" --exclude="data/warc_files" --exclude="data/webgraph" -zcvf  data.tar.gz data
