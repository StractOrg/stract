#![no_main]

use libfuzzer_sys::fuzz_target;
use stract::feed;

fuzz_target!(|data: &str| {
    let _ = feed::parse(data, feed::FeedKind::Atom);
});
