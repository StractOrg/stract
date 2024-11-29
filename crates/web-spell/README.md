# Web Spell

Automatic spelling correction from web data. It is based on the paper
[Using the Web for Language Independent Spellchecking and
Autocorrection](http://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/36180.pdf)
from google.

## Usage
```rust
let checker = SpellChecker::open("<path-to-model>", CorrectionConfig::default()).unwrap();
let correction = checker.correct("hwllo", Lang::Eng);
assert_eq!(correction.unwrap().terms, vec![CorrectionTerm::Corrected { orig: "hwllo".to_string(), correction: "hello".to_string() }]);
```
