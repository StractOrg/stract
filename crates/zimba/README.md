# Zimba
Zimba is a parser for the [Zim file format](https://openzim.org/wiki/ZIM_file_format) written in pure Rust. Zim files are commonly used e.g. by wikipedia to distribute dumps of their articles for offline usage. Zimba only intends to support reading of Zim files, not writing.

## Usage
```rust
use zimba::{ZimFile, Error};

fn main() -> Result<(), Error> {
    let zim_file = ZimFile::open("path/to/file.zim")?;

    for article in zim_file.articles()? {
        println!("{}", article.title);
    }

    Ok(())
}
```