@dev *ARGS:
    cd frontend && npm install
    ./scripts/run_dev.py {{ARGS}}

export RUST_LOG := env_var_or_default("RUST_LOG", "info,stract=debug")

export STRACT_CARGO_ARGS := env_var_or_default("STRACT_CARGO_ARGS", "")

@dev-api:
    cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- api configs/api.toml"
@dev-search-server:
    cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- search-server configs/search_server.toml"
@dev-webgraph:
    cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- webgraph server configs/webgraph/server.toml"
@dev-alice:
    cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- alice serve configs/alice.toml"
@dev-frontend:
    cd frontend && npm run dev

@openapi:
    cd frontend && npm run openapi

@setup *ARGS:
    just setup_python_env
    just download_libtorch {{ARGS}}
    ./scripts/export_crossencoder
    ./scripts/export_qa_model
    ./scripts/export_abstractive_summary_model
    ./scripts/export_dual_encoder
    ./scripts/export_fact_model

@configure *ARGS:
    just setup {{ARGS}}
    RUST_LOG="none,stract=info" just cargo run --release --all-features -- configure {{ARGS}}

@setup_python_env:
    python3 -m venv .venv || true
    .venv/bin/pip install -r scripts/requirements.txt

@download_libtorch *ARGS:
    .venv/bin/python3 scripts/download_libtorch.py {{ARGS}}

@cargo *ARGS:
    LIBTORCH="{{justfile_directory()}}/libtorch" LD_LIBRARY_PATH="{{justfile_directory()}}/libtorch/lib" DYLD_LIBRARY_PATH="{{justfile_directory()}}/libtorch/lib" cargo {{ARGS}}
