// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use pyo3::prelude::*;

static TOKIO_RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    });

#[pymodule]
mod leechy {
    use super::*;
    use ::leechy as lchy;

    #[pyclass]
    struct Engine {
        inner: lchy::Engine,
    }

    #[pymethods]
    impl Engine {
        #[new]
        #[pyo3(signature = (name = "google"))]
        fn new(name: &str) -> PyResult<Self> {
            match lchy::Engine::by_name(name) {
                Some(engine) => Ok(Self { inner: engine }),
                None => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unknown engine: {}",
                    name
                ))),
            }
        }

        fn search(&self, query: String) -> PyResult<Vec<String>> {
            TOKIO_RUNTIME.block_on(async {
                self.inner
                    .search(query.as_str())
                    .await
                    .map_err(|e| pyo3::exceptions::PyException::new_err(format!("{}", e)))
                    .map(|res| res.into_iter().map(|r| r.to_string()).collect())
            })
        }
    }
}
