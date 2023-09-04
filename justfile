@frontend-rerun *ARGS:
    ./scripts/run_frontend.py {{ARGS}}

@frontend *ARGS:
    cargo watch -i frontend -s 'just frontend-rerun {{ARGS}}'

@frontend-openapi:
    deno run -A npm:openapi-typescript http://localhost:3000/beta/api/docs/openapi.json -o frontend/search/schema.d.ts

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
    just cargo run --release --all-features -- configure {{ARGS}}

@setup_python_env:
    python3 -m venv .venv || true
    .venv/bin/pip install -r scripts/requirements.txt

@download_libtorch *ARGS:
    .venv/bin/python3 scripts/download_libtorch.py {{ARGS}}

@cargo *ARGS:
    LIBTORCH="{{justfile_directory()}}/libtorch" LD_LIBRARY_PATH="{{justfile_directory()}}/libtorch/lib" DYLD_LIBRARY_PATH="{{justfile_directory()}}/libtorch/lib" cargo {{ARGS}}
