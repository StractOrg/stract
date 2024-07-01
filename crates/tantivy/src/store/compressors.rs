use std::io;

use serde::{Deserialize, Deserializer, Serialize};

/// Compressor can be used on `IndexSettings` to choose
/// the compressor used to compress the doc store.
///
/// The default is Lz4Block, but also depends on the enabled feature flags.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum Compressor {
    /// No compression
    None,
    /// Use the lz4 compressor (block format)
    #[cfg(feature = "lz4-compression")]
    Lz4,
}

impl Serialize for Compressor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            Compressor::None => serializer.serialize_str("none"),
            #[cfg(feature = "lz4-compression")]
            Compressor::Lz4 => serializer.serialize_str("lz4"),
        }
    }
}

impl<'de> Deserialize<'de> for Compressor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        let compressor =
            match buf.as_str() {
                "none" => Compressor::None,
                #[cfg(feature = "lz4-compression")]
                "lz4" => Compressor::Lz4,
                #[cfg(not(feature = "lz4-compression"))]
                "lz4" => return Err(serde::de::Error::custom(
                    "unsupported variant `lz4`, please enable Tantivy's `lz4-compression` feature",
                )),
                _ => {
                    return Err(serde::de::Error::unknown_variant(
                        &buf,
                        &[
                            "none",
                            #[cfg(feature = "lz4-compression")]
                            "lz4",
                        ],
                    ));
                }
            };

        Ok(compressor)
    }
}

impl Default for Compressor {
    #[allow(unreachable_code)]
    fn default() -> Self {
        #[cfg(feature = "lz4-compression")]
        return Compressor::Lz4;

        Compressor::None
    }
}

impl Compressor {
    #[inline]
    pub(crate) fn compress_into(
        &self,
        uncompressed: &[u8],
        compressed: &mut Vec<u8>,
    ) -> io::Result<()> {
        match self {
            Self::None => {
                compressed.clear();
                compressed.extend_from_slice(uncompressed);
                Ok(())
            }
            #[cfg(feature = "lz4-compression")]
            Self::Lz4 => super::compression_lz4_block::compress(uncompressed, compressed),
        }
    }
}
