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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! This has been copied from `bincode` so that we can derive Clone, serde::Serialize and serde::Deserialize for `Compat`
#[derive(Clone, serde::Serialize, serde::Deserialize, thiserror::Error)]
pub struct SerdeCompat<T>(pub T);

impl<T> bincode::Decode for SerdeCompat<T>
where
    T: serde::de::DeserializeOwned,
{
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let serde_decoder = de_owned::SerdeDecoder { de: decoder };
        T::deserialize(serde_decoder).map(SerdeCompat)
    }
}
impl<'de, T> bincode::BorrowDecode<'de> for SerdeCompat<T>
where
    T: serde::de::DeserializeOwned,
{
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let serde_decoder = de_owned::SerdeDecoder { de: decoder };
        T::deserialize(serde_decoder).map(SerdeCompat)
    }
}

impl<T> bincode::Encode for SerdeCompat<T>
where
    T: serde::Serialize,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        let serializer = ser::SerdeEncoder { enc: encoder };
        self.0.serialize(serializer)?;
        Ok(())
    }
}

impl<T> core::fmt::Debug for SerdeCompat<T>
where
    T: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Compat").field(&self.0).finish()
    }
}

impl<T> core::fmt::Display for SerdeCompat<T>
where
    T: core::fmt::Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

mod de_owned {
    use bincode::serde::DecodeError as SerdeDecodeError;
    use bincode::{
        de::{Decode, Decoder},
        error::DecodeError,
    };
    use serde::de::*;

    #[inline]
    pub(crate) fn decode_option_variant<D: Decoder>(
        decoder: &mut D,
        type_name: &'static str,
    ) -> Result<Option<()>, DecodeError> {
        let is_some = u8::decode(decoder)?;
        match is_some {
            0 => Ok(None),
            1 => Ok(Some(())),
            x => Err(DecodeError::UnexpectedVariant {
                found: x as u32,
                allowed: &bincode::error::AllowedEnumVariants::Range { max: 1, min: 0 },
                type_name,
            }),
        }
    }

    pub(crate) struct SerdeDecoder<'a, DE: Decoder> {
        pub(crate) de: &'a mut DE,
    }

    impl<'de, DE: Decoder> Deserializer<'de> for SerdeDecoder<'_, DE> {
        type Error = DecodeError;

        fn deserialize_any<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            Err(SerdeDecodeError::AnyNotSupported.into())
        }

        fn deserialize_bool<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_bool(Decode::decode(&mut self.de)?)
        }

        fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_i8(Decode::decode(&mut self.de)?)
        }

        fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_i16(Decode::decode(&mut self.de)?)
        }

        fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_i32(Decode::decode(&mut self.de)?)
        }

        fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_i64(Decode::decode(&mut self.de)?)
        }

        serde::serde_if_integer128! {
            fn deserialize_i128<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: serde::de::Visitor<'de>,
            {
                visitor.visit_i128(Decode::decode(&mut self.de)?)
            }
        }

        fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_u8(Decode::decode(&mut self.de)?)
        }

        fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_u16(Decode::decode(&mut self.de)?)
        }

        fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_u32(Decode::decode(&mut self.de)?)
        }

        fn deserialize_u64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_u64(Decode::decode(&mut self.de)?)
        }

        serde::serde_if_integer128! {
            fn deserialize_u128<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: serde::de::Visitor<'de>,
            {
                visitor.visit_u128(Decode::decode(&mut self.de)?)
            }
        }

        fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_f32(Decode::decode(&mut self.de)?)
        }

        fn deserialize_f64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_f64(Decode::decode(&mut self.de)?)
        }

        fn deserialize_char<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_char(Decode::decode(&mut self.de)?)
        }

        fn deserialize_str<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            Err(SerdeDecodeError::CannotBorrowOwnedData.into())
        }

        fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_string(Decode::decode(&mut self.de)?)
        }

        fn deserialize_bytes<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_byte_buf(Decode::decode(&mut self.de)?)
        }

        fn deserialize_byte_buf<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_byte_buf(Decode::decode(&mut self.de)?)
        }

        fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            let variant = decode_option_variant(&mut self.de, "Option<T>")?;
            if variant.is_some() {
                visitor.visit_some(self)
            } else {
                visitor.visit_none()
            }
        }

        fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_unit()
        }

        fn deserialize_unit_struct<V>(
            self,
            _name: &'static str,
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_unit()
        }

        fn deserialize_newtype_struct<V>(
            self,
            _name: &'static str,
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_newtype_struct(self)
        }

        fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            let len = usize::decode(&mut self.de)?;
            self.deserialize_tuple(len, visitor)
        }

        fn deserialize_tuple<V>(mut self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            struct Access<'a, 'b, DE: Decoder> {
                deserializer: &'a mut SerdeDecoder<'b, DE>,
                len: usize,
            }

            impl<'de, 'a, 'b: 'a, DE: Decoder + 'b> SeqAccess<'de> for Access<'a, 'b, DE> {
                type Error = DecodeError;

                fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, DecodeError>
                where
                    T: DeserializeSeed<'de>,
                {
                    if self.len > 0 {
                        self.len -= 1;
                        let value = DeserializeSeed::deserialize(
                            seed,
                            SerdeDecoder {
                                de: self.deserializer.de,
                            },
                        )?;
                        Ok(Some(value))
                    } else {
                        Ok(None)
                    }
                }

                fn size_hint(&self) -> Option<usize> {
                    Some(self.len)
                }
            }

            visitor.visit_seq(Access {
                deserializer: &mut self,
                len,
            })
        }

        fn deserialize_tuple_struct<V>(
            self,
            _name: &'static str,
            len: usize,
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            self.deserialize_tuple(len, visitor)
        }

        fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            struct Access<'a, 'b, DE: Decoder> {
                deserializer: &'a mut SerdeDecoder<'b, DE>,
                len: usize,
            }

            impl<'de, 'a, 'b: 'a, DE: Decoder + 'b> MapAccess<'de> for Access<'a, 'b, DE> {
                type Error = DecodeError;

                fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, DecodeError>
                where
                    K: DeserializeSeed<'de>,
                {
                    if self.len > 0 {
                        self.len -= 1;
                        let key = DeserializeSeed::deserialize(
                            seed,
                            SerdeDecoder {
                                de: self.deserializer.de,
                            },
                        )?;
                        Ok(Some(key))
                    } else {
                        Ok(None)
                    }
                }

                fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, DecodeError>
                where
                    V: DeserializeSeed<'de>,
                {
                    let value = DeserializeSeed::deserialize(
                        seed,
                        SerdeDecoder {
                            de: self.deserializer.de,
                        },
                    )?;
                    Ok(value)
                }

                fn size_hint(&self) -> Option<usize> {
                    Some(self.len)
                }
            }

            let len = usize::decode(&mut self.de)?;

            visitor.visit_map(Access {
                deserializer: &mut self,
                len,
            })
        }

        fn deserialize_struct<V>(
            self,
            _name: &'static str,
            fields: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            self.deserialize_tuple(fields.len(), visitor)
        }

        fn deserialize_enum<V>(
            self,
            _name: &'static str,
            _variants: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_enum(self)
        }

        fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            Err(SerdeDecodeError::IdentifierNotSupported.into())
        }

        fn deserialize_ignored_any<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            Err(SerdeDecodeError::IgnoredAnyNotSupported.into())
        }

        fn is_human_readable(&self) -> bool {
            false
        }
    }

    impl<'de, DE: Decoder> EnumAccess<'de> for SerdeDecoder<'_, DE> {
        type Error = DecodeError;
        type Variant = Self;

        fn variant_seed<V>(mut self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
        where
            V: DeserializeSeed<'de>,
        {
            let idx = u32::decode(&mut self.de)?;
            let val = seed.deserialize(idx.into_deserializer())?;
            Ok((val, self))
        }
    }

    impl<'de, DE: Decoder> VariantAccess<'de> for SerdeDecoder<'_, DE> {
        type Error = DecodeError;

        fn unit_variant(self) -> Result<(), Self::Error> {
            Ok(())
        }

        fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
        where
            T: DeserializeSeed<'de>,
        {
            DeserializeSeed::deserialize(seed, self)
        }

        fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            Deserializer::deserialize_tuple(self, len, visitor)
        }

        fn struct_variant<V>(
            self,
            fields: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            Deserializer::deserialize_tuple(self, fields.len(), visitor)
        }
    }
}

mod ser {
    use bincode::serde::EncodeError as SerdeEncodeError;
    use bincode::{
        enc::{Encode, Encoder},
        error::EncodeError,
    };
    use serde::ser::*;

    // /// Encode the given value into a `Vec<u8>` with the given `Config`. See the [config] module for more information.
    // ///
    // /// [config]: ../config/index.html
    // #[cfg(feature = "alloc")]
    // #[cfg_attr(docsrs, doc(cfg(feature = "alloc")))]
    // pub fn encode_to_vec<E, C>(val: E, config: C) -> Result<Vec<u8>, EncodeError>
    // where
    //     E: Serialize,
    //     C: Config,
    // {
    //     let mut encoder = bincode::enc::EncoderImpl::new(bincode::VecWriter::default(), config);
    //     let serializer = SerdeEncoder { enc: &mut encoder };
    //     val.serialize(serializer)?;
    //     Ok(encoder.into_writer().collect())
    // }

    // /// Encode the given value into the given slice. Returns the amount of bytes that have been written.
    // ///
    // /// See the [config] module for more information on configurations.
    // ///
    // /// [config]: ../config/index.html
    // pub fn encode_into_slice<E, C>(val: E, dst: &mut [u8], config: C) -> Result<usize, EncodeError>
    // where
    //     E: Serialize,
    //     C: Config,
    // {
    //     let mut encoder =
    //         bincode::enc::EncoderImpl::new(bincode::enc::write::SliceWriter::new(dst), config);
    //     let serializer = SerdeEncoder { enc: &mut encoder };
    //     val.serialize(serializer)?;
    //     Ok(encoder.into_writer().bytes_written())
    // }

    // /// Encode the given value into a custom [Writer].
    // ///
    // /// See the [config] module for more information on configurations.
    // ///
    // /// [config]: ../config/index.html
    // pub fn encode_into_writer<E: Serialize, W: Writer, C: Config>(
    //     val: E,
    //     writer: W,
    //     config: C,
    // ) -> Result<(), EncodeError> {
    //     let mut encoder = bincode::enc::EncoderImpl::<_, C>::new(writer, config);
    //     let serializer = SerdeEncoder { enc: &mut encoder };
    //     val.serialize(serializer)?;
    //     Ok(())
    // }

    // /// Encode the given value into any type that implements `std::io::Write`, e.g. `std::fs::File`, with the given `Config`.
    // /// See the [config] module for more information.
    // ///
    // /// [config]: ../config/index.html
    // #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    // #[cfg(feature = "std")]
    // pub fn encode_into_std_write<E: Serialize, C: Config, W: std::io::Write>(
    //     val: E,
    //     dst: &mut W,
    //     config: C,
    // ) -> Result<usize, EncodeError> {
    //     let writer = bincode::IoWriter::new(dst);
    //     let mut encoder = bincode::enc::EncoderImpl::<_, C>::new(writer, config);
    //     let serializer = SerdeEncoder { enc: &mut encoder };
    //     val.serialize(serializer)?;
    //     Ok(encoder.into_writer().bytes_written())
    // }

    pub(super) struct SerdeEncoder<'a, ENC: Encoder> {
        pub(super) enc: &'a mut ENC,
    }

    impl<ENC> Serializer for SerdeEncoder<'_, ENC>
    where
        ENC: Encoder,
    {
        type Ok = ();

        type Error = EncodeError;

        type SerializeSeq = Self;
        type SerializeTuple = Self;
        type SerializeTupleStruct = Self;
        type SerializeTupleVariant = Self;
        type SerializeMap = Self;
        type SerializeStruct = Self;
        type SerializeStructVariant = Self;

        fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        serde::serde_if_integer128! {
            fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
                v.encode(self.enc)
            }
        }

        fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        serde::serde_if_integer128! {
            fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
                v.encode(self.enc)
            }
        }

        fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
            v.encode(self.enc)
        }

        fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
            0u8.encode(self.enc)
        }

        fn serialize_some<T>(mut self, value: &T) -> Result<Self::Ok, Self::Error>
        where
            T: ?Sized + Serialize,
        {
            1u8.encode(&mut self.enc)?;
            value.serialize(self)
        }

        fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }

        fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }

        fn serialize_unit_variant(
            self,
            _name: &'static str,
            variant_index: u32,
            _variant: &'static str,
        ) -> Result<Self::Ok, Self::Error> {
            variant_index.encode(self.enc)
        }

        fn serialize_newtype_struct<T>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<Self::Ok, Self::Error>
        where
            T: Serialize + ?Sized,
        {
            value.serialize(self)
        }

        fn serialize_newtype_variant<T>(
            mut self,
            _name: &'static str,
            variant_index: u32,
            _variant: &'static str,
            value: &T,
        ) -> Result<Self::Ok, Self::Error>
        where
            T: Serialize + ?Sized,
        {
            variant_index.encode(&mut self.enc)?;
            value.serialize(self)
        }

        fn serialize_seq(mut self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
            let len = len.ok_or_else(|| SerdeEncodeError::SequenceMustHaveLength.into())?;
            len.encode(&mut self.enc)?;
            Ok(Compound { enc: self.enc })
        }

        fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
            Ok(self)
        }

        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct, Self::Error> {
            Ok(Compound { enc: self.enc })
        }

        fn serialize_tuple_variant(
            mut self,
            _name: &'static str,
            variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant, Self::Error> {
            variant_index.encode(&mut self.enc)?;
            Ok(Compound { enc: self.enc })
        }

        fn serialize_map(mut self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
            let len = len.ok_or_else(|| SerdeEncodeError::SequenceMustHaveLength.into())?;
            len.encode(&mut self.enc)?;
            Ok(Compound { enc: self.enc })
        }

        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct, Self::Error> {
            Ok(Compound { enc: self.enc })
        }

        fn serialize_struct_variant(
            mut self,
            _name: &'static str,
            variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant, Self::Error> {
            variant_index.encode(&mut self.enc)?;
            Ok(Compound { enc: self.enc })
        }

        fn is_human_readable(&self) -> bool {
            false
        }
    }

    type Compound<'a, ENC> = SerdeEncoder<'a, ENC>;

    impl<ENC: Encoder> SerializeSeq for Compound<'_, ENC> {
        type Ok = ();
        type Error = EncodeError;

        fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: Serialize + ?Sized,
        {
            value.serialize(SerdeEncoder { enc: self.enc })
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<ENC: Encoder> SerializeTuple for Compound<'_, ENC> {
        type Ok = ();
        type Error = EncodeError;

        fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: Serialize + ?Sized,
        {
            value.serialize(SerdeEncoder { enc: self.enc })
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<ENC: Encoder> SerializeTupleStruct for Compound<'_, ENC> {
        type Ok = ();
        type Error = EncodeError;

        fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: Serialize + ?Sized,
        {
            value.serialize(SerdeEncoder { enc: self.enc })
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<ENC: Encoder> SerializeTupleVariant for Compound<'_, ENC> {
        type Ok = ();
        type Error = EncodeError;

        fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: Serialize + ?Sized,
        {
            value.serialize(SerdeEncoder { enc: self.enc })
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<ENC: Encoder> SerializeMap for Compound<'_, ENC> {
        type Ok = ();
        type Error = EncodeError;

        fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
        where
            T: Serialize + ?Sized,
        {
            key.serialize(SerdeEncoder { enc: self.enc })
        }

        fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: Serialize + ?Sized,
        {
            value.serialize(SerdeEncoder { enc: self.enc })
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<ENC: Encoder> SerializeStruct for Compound<'_, ENC> {
        type Ok = ();
        type Error = EncodeError;

        fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error>
        where
            T: Serialize + ?Sized,
        {
            value.serialize(SerdeEncoder { enc: self.enc })
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<ENC: Encoder> SerializeStructVariant for Compound<'_, ENC> {
        type Ok = ();
        type Error = EncodeError;

        fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error>
        where
            T: Serialize + ?Sized,
        {
            value.serialize(SerdeEncoder { enc: self.enc })
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }
}
