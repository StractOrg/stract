@profile-indexer:
    sudo rm -rf data/index
    just cargo flamegraph --root -- indexer local configs/indexer/profile.toml

@webgraph:
    rm -rf data/webgraph
    just cargo run --release -- webgraph local configs/webgraph/local.toml

@frontend-rerun *ARGS:
    cd frontend; npm run build
    ./scripts/run_frontend.py {{ARGS}}

@frontend *ARGS:
    cd frontend; npm install
    cargo watch -s 'just frontend-rerun {{ARGS}}'

@astro:
    cd frontend; npm run dev

@setup *ARGS:
    just setup_python_env
    just download_libtorch {{ARGS}}
    ./scripts/export_crossencoder
    ./scripts/export_qa_model
    ./scripts/export_abstractive_summary_model
    ./scripts/export_dual_encoder
    ./scripts/export_fact_model
    cd frontend; npm install; npm run build

@configure *ARGS:
    just setup {{ARGS}}
    just cargo run --release --all-features -- configure {{ARGS}}

@centrality webgraph output:
    ./scripts/build_harmonic {{webgraph}} {{output}}

@entity:
    rm -rf data/entity
    just cargo run --release -- indexer entity data/enwiki_subset.xml.bz2 data/entity

@setup_python_env:
    rm -rf .venv
    python3 -m venv .venv
    .venv/bin/pip install -r scripts/requirements.txt

@download_libtorch *ARGS:
    .venv/bin/python3 scripts/download_libtorch.py {{ARGS}}

@cargo *ARGS:
    LIBTORCH="{{justfile_directory()}}/libtorch" LD_LIBRARY_PATH="{{justfile_directory()}}/libtorch/lib" DYLD_LIBRARY_PATH="{{justfile_directory()}}/libtorch/lib" cargo {{ARGS}}