// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use wasm_bindgen::prelude::*;
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log(s: &str);
}

fn log(s: &str) {
    unsafe { console_log(&("[optics] ".to_owned() + s)) }
}

#[wasm_bindgen]
pub struct OpticsBackend {}

#[wasm_bindgen]
impl OpticsBackend {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        // console_error_panic_hook::set_once();
        log("WHUT");

        Self {}
    }

    // #[wasm_bindgen(js_name = onNotification)]
    // pub fn on_notification(&mut self, method: &str, params: JsValue) {
    //     println!("KAGE");
    //     match method {
    //         _ => println!("on_notification {} {:?}", method, params),
    //     }
    // }
}
