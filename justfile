@profile-indexer:
    sudo rm -rf data/index
    cargo flamegraph --root -- indexer local configs/indexer/profile.toml

@webgraph:
    rm -rf data/webgraph
    cargo run --release -- webgraph local configs/webgraph/local.toml

@frontend-rerun:
    # To inspect timings use the commands below instead:
    # time ((cd frontend; npm run build) && cargo build)
    # cargo run -- frontend data/index data/queries_us.csv data/entity data/bangs.json
    cd frontend; npm run build
    bash scripts/run_frontend.sh

@frontend:
    cd frontend; npm install
    cargo watch -s 'just frontend-rerun'

@astro:
    cd frontend; npm run dev

@configure:
    cd frontend; npm install; npm run build
    cargo run --release --all-features -- configure

@entity:
    rm -rf data/entity
    cargo run --release -- indexer entity data/enwiki_subset.xml.bz2 data/entity
