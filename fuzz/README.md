Fuzz robots.txt parser:
`RUSTFLAGS="-C codegen-units=16" cargo +nightly fuzz run --jobs 16 -O --no-cfg-fuzzing robotstxt -- -dict=fuzz/dicts/robots_txt.dict -max-len=2048`

Fuzz query parser:
`RUSTFLAGS="-C codegen-units=16" cargo +nightly fuzz run --jobs 16 -O --no-cfg-fuzzing query-parser -- -max-len=2048`

Fuzz RSS parser:
`RUSTFLAGS="-C codegen-units=16" cargo +nightly fuzz run --jobs 16 -O --no-cfg-fuzzing rss -- -max-len=2048`

Fuzz Atom parser:
`RUSTFLAGS="-C codegen-units=16" cargo +nightly fuzz run --jobs 16 -O --no-cfg-fuzzing atom -- -max-len=2048`