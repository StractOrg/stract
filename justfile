@dev *ARGS:
    cd frontend && npm install
    ./scripts/run_dev.py {{ARGS}}

export RUST_LOG := env_var_or_default("RUST_LOG", "info,stract=debug")

export STRACT_CARGO_ARGS := env_var_or_default("STRACT_CARGO_ARGS", "")

@dev-api:
    cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- api configs/api.toml"
@dev-search-server:
    cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- search-server configs/search_server.toml"
@dev-entity-search-server:
    cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- entity-search-server configs/entity_search_server.toml"
@dev-webgraph:
    cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- webgraph server configs/webgraph/host_server.toml" && cargo watch -i frontend -x "run $STRACT_CARGO_ARGS -- webgraph server configs/webgraph/page_server.toml"
@dev-llm:
    ./.venv/bin/python3 -m llama_cpp.server --model data/mistral-7b-instruct-v0.2.Q4_K_M.gguf --port 4000
@dev-frontend:
    cd frontend && npm run dev

@openapi:
    cd frontend && npm run openapi

@setup *ARGS:
    python3 -m venv .venv || true
    just download_libtorch {{ARGS}}

@prepare_models:
    just setup_python_env
    ./scripts/export_crossencoder
    ./scripts/export_abstractive_summary_model
    ./scripts/export_dual_encoder

@configure *ARGS:
    just setup {{ARGS}}
    just prepare_models
    RUST_LOG="none,stract=info" just cargo run --release --all-features -- configure {{ARGS}}

@setup_python_env:
    python3 -m venv .venv || true
    .venv/bin/pip install -r scripts/requirements.txt

@download_libtorch *ARGS:
    .venv/bin/python3 scripts/download_libtorch.py {{ARGS}}

@cargo *ARGS:
    LIBTORCH="{{justfile_directory()}}/libtorch" LD_LIBRARY_PATH="{{justfile_directory()}}/libtorch/lib" DYLD_LIBRARY_PATH="{{justfile_directory()}}/libtorch/lib" cargo {{ARGS}}
