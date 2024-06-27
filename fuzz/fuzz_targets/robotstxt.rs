#![no_main]

use libfuzzer_sys::fuzz_target;
use robotstxt::Robots;

fuzz_target!(|data: &str| {
    let _ = Robots::parse("FooBot", data);
});
