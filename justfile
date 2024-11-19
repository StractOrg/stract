@dev *ARGS:
    cd frontend && npm install
    ./scripts/run_dev.py {{ARGS}}

export RUST_LOG := env_var_or_default("RUST_LOG", "stract=debug")

export STRACT_CARGO_ARGS := env_var_or_default("STRACT_CARGO_ARGS", "")

@dev-api:
    cargo watch -i frontend -i tools -i crates/client-wasm -i docs -x "run $STRACT_CARGO_ARGS -- api configs/api.toml"
@dev-search-server:
    cargo watch -i frontend -i tools -i crates/client-wasm -i docs -x "run $STRACT_CARGO_ARGS -- search-server configs/search_server.toml"
@dev-entity-search-server:
    cargo watch -i frontend -i tools -i crates/client-wasm -i docs -x "run $STRACT_CARGO_ARGS -- entity-search-server configs/entity_search_server.toml"
@dev-webgraph:
    cargo watch -i frontend -i tools -i crates/client-wasm -i docs -x "run $STRACT_CARGO_ARGS -- webgraph server configs/webgraph/server.toml"
@dev-frontend:
    cd frontend && npm run dev

@openapi:
    cd frontend && npm run openapi

@setup *ARGS:
    just setup_python_env
    ./scripts/setup {{ARGS}}

@configure *ARGS:
    just setup {{ARGS}}
    RUST_LOG="none,stract=info" cargo run --release --all-features -- configure {{ARGS}}

@setup_python_env:
    python3 -m venv .venv || true

@update:
    cargo update
    cd crates/leechy-py && cargo update
    cd frontend && npm update
    cd crates/optics-lsp && npm update
    cd tools/annotate-results && npm update
    cd tools/ranking-diff && npm update