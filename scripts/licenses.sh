#!/bin/bash
set -e
cargo about generate --fail -c scripts/licenses/licenses.toml scripts/licenses/template.hbs > assets/licenses.html
