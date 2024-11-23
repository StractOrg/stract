use core::fmt;
use std::{
    borrow::Borrow,
    io::{self, BufWriter, Write},
};

use ownedbytes::OwnedBytes;
use strum::EnumDiscriminants;
use thiserror::Error;

pub mod readers;
pub mod writer;

#[derive(Debug, Error)]
pub enum RowOrderError {
    #[error("Error occurred during IO operation: {0}")]
    Io(#[from] io::Error),
}

type Result<T, E = RowOrderError> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    name: String,
    id: u32,
    typ: RowValueType,
}

impl Field {
    pub fn new(name: String, id: u32, typ: RowValueType) -> Self {
        Self { name, id, typ }
    }

    pub fn row_value_type(&self) -> RowValueType {
        self.typ
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    fn write_to(&self, buf: &mut Vec<u8>) {
        buf.clear();
        let serialized_name = self.name.as_bytes();
        let len = serialized_name.len() as u32;
        let serialized_len = len.to_le_bytes();
        let serialized_id = self.id.to_le_bytes();
        let serialized_typ = self.typ.id().to_le_bytes();

        let total_bytes = serialized_name.len()
            + serialized_len.len()
            + serialized_id.len()
            + serialized_typ.len();

        buf.resize(total_bytes, 0);

        let mut offset = 0;
        buf[offset..offset + serialized_len.len()].copy_from_slice(&serialized_len);
        offset += serialized_len.len();
        buf[offset..offset + serialized_name.len()].copy_from_slice(serialized_name);
        offset += serialized_name.len();
        buf[offset..offset + serialized_id.len()].copy_from_slice(&serialized_id);
        offset += serialized_id.len();
        buf[offset..offset + serialized_typ.len()].copy_from_slice(&serialized_typ);
    }

    fn read_from(bytes: &[u8]) -> Result<(Self, usize)> {
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let name = String::from_utf8(bytes[4..4 + len].to_vec()).unwrap();
        let id = u32::from_le_bytes(bytes[4 + len..8 + len].try_into().unwrap());
        let typ = RowValueType::from_id(u32::from_le_bytes(
            bytes[8 + len..12 + len].try_into().unwrap(),
        ));

        Ok((Self { name, id, typ }, 12 + len))
    }
}

struct Header {
    field_order: Vec<Field>,
    row_size_bytes: usize,
}

impl Header {
    fn row_types(&self) -> impl Iterator<Item = RowValueType> + '_ {
        self.field_order.iter().map(|field| field.row_value_type())
    }

    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();
        let mut buf = Vec::new();

        res.extend_from_slice(&(self.row_size_bytes as u32).to_le_bytes());

        for field in &self.field_order {
            field.write_to(&mut buf);
            res.extend_from_slice(&buf);
        }

        res
    }

    fn deserialize(bytes: &[u8]) -> Result<Self> {
        let row_size_bytes = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let mut offset = 4;
        let mut field_order = Vec::new();

        while offset < bytes.len() {
            let (field, bytes_read) = Field::read_from(&bytes[offset..])?;
            field_order.push(field);
            offset += bytes_read;
        }

        Ok(Self {
            field_order,
            row_size_bytes,
        })
    }

    fn row_index(&self) -> Vec<Option<usize>> {
        if self.field_order.is_empty() {
            return vec![];
        }

        let max_id = self
            .field_order
            .iter()
            .map(|field| field.id())
            .max()
            .unwrap();

        let mut res = vec![None; max_id as usize + 1];

        for (idx, field) in self.field_order.iter().enumerate() {
            res[field.id() as usize] = Some(idx);
        }

        res
    }

    fn field_order(&self) -> &[Field] {
        &self.field_order
    }

    fn row_size_bytes(&self) -> usize {
        self.row_size_bytes
    }
}

#[derive(EnumDiscriminants, Debug, Clone, Copy, PartialEq)]
#[strum_discriminants(name(RowValueType))]
pub enum RowValue {
    U64(u64),
    U128(u128),
    I64(i64),
    F64(f64),
    Bool(bool),
}

impl RowValueType {
    const fn size(&self) -> usize {
        match self {
            RowValueType::U64 => 8,
            RowValueType::U128 => 16,
            RowValueType::F64 => 8,
            RowValueType::I64 => 8,
            RowValueType::Bool => 1,
        }
    }

    fn id(&self) -> u32 {
        match self {
            RowValueType::U64 => 0,
            RowValueType::F64 => 1,
            RowValueType::I64 => 2,
            RowValueType::Bool => 3,
            RowValueType::U128 => 4,
        }
    }

    fn from_id(id: u32) -> Self {
        match id {
            0 => RowValueType::U64,
            1 => RowValueType::F64,
            2 => RowValueType::I64,
            3 => RowValueType::Bool,
            4 => RowValueType::U128,
            _ => panic!("Invalid id"),
        }
    }
}

impl RowValue {
    fn write_to(&self, buf: &mut [u8]) {
        match self {
            RowValue::U64(v) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            RowValue::F64(v) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            RowValue::I64(v) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            RowValue::Bool(v) => {
                buf[0] = *v as u8;
            }
            RowValue::U128(v) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
        }
    }

    fn read_from(buf: &[u8], typ: RowValueType) -> Self {
        match typ {
            RowValueType::U64 => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[..typ.size()]);
                RowValue::U64(u64::from_le_bytes(bytes))
            }
            RowValueType::F64 => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[..typ.size()]);
                RowValue::F64(f64::from_le_bytes(bytes))
            }
            RowValueType::I64 => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[..typ.size()]);
                RowValue::I64(i64::from_le_bytes(bytes))
            }

            RowValueType::Bool => RowValue::Bool(buf[0] != 0),

            RowValueType::U128 => {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&buf[..typ.size()]);
                RowValue::U128(u128::from_le_bytes(bytes))
            }
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            RowValue::U64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            RowValue::I64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            RowValue::F64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            RowValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u128(&self) -> Option<u128> {
        match self {
            RowValue::U128(v) => Some(*v),
            _ => None,
        }
    }
}

pub struct Row {
    index: Vec<Option<usize>>,
    values: Vec<RowValue>,
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row").field("values", &self.values).finish()
    }
}

pub trait AsFieldId {
    fn field_id(&self) -> u32;
}

impl AsFieldId for Field {
    fn field_id(&self) -> u32 {
        self.id()
    }
}

impl AsFieldId for crate::schema::Field {
    fn field_id(&self) -> u32 {
        crate::schema::Field::field_id(*self)
    }
}

impl AsFieldId for u32 {
    fn field_id(&self) -> u32 {
        *self
    }
}

impl Row {
    pub fn get<F: AsFieldId>(&self, field: &F) -> Option<&RowValue> {
        self.get_by_field_id(field.field_id())
    }

    pub fn get_by_field_id(&self, field_id: u32) -> Option<&RowValue> {
        self.index
            .get(field_id as usize)
            .copied()
            .flatten()
            .map(|idx| &self.values[idx])
    }

    pub fn get_u64<F: AsFieldId>(&self, field: &F) -> Option<u64> {
        self.get(field).and_then(|v| v.as_u64())
    }

    pub fn get_u128<F: AsFieldId>(&self, field: &F) -> Option<u128> {
        self.get(field).and_then(|v| v.as_u128())
    }

    pub fn get_i64<F: AsFieldId>(&self, field: &F) -> Option<i64> {
        self.get(field).and_then(|v| v.as_i64())
    }

    pub fn get_f64<F: AsFieldId>(&self, field: &F) -> Option<f64> {
        self.get(field).and_then(|v| v.as_f64())
    }

    pub fn get_bool<F: AsFieldId>(&self, field: &F) -> Option<bool> {
        self.get(field).and_then(|v| v.as_bool())
    }
}

pub struct RowIndexer<W: Write> {
    header: Header,
    writer: BufWriter<W>,
    buf: Vec<u8>,
}

impl<W> RowIndexer<W>
where
    W: Write,
{
    pub fn new(writer: W, field_order: Vec<Field>) -> io::Result<Self> {
        let row_size_bytes = field_order
            .iter()
            .map(|field| field.row_value_type().size())
            .sum();

        let header = Header {
            field_order,
            row_size_bytes,
        };

        let mut writer = BufWriter::new(writer);

        let serialized_header = header.serialize();
        let len = (serialized_header.len() as u64).to_le_bytes();
        writer.write_all(&len)?;
        writer.write_all(&serialized_header)?;
        writer.flush()?;

        Ok(Self {
            header,
            writer,
            buf: vec![0; row_size_bytes],
        })
    }

    pub fn write_row(&mut self, values: &[RowValue]) {
        debug_assert_eq!(
            values.len(),
            self.header.field_order.len(),
            "Row has wrong number of values"
        );

        debug_assert_eq!(
            values
                .iter()
                .map(|v| RowValueType::from(v).size())
                .sum::<usize>(),
            self.header.row_size_bytes,
            "Row has wrong size"
        );

        self.buf.clear();
        self.buf.resize(self.header.row_size_bytes, 0);

        let mut prev_end = 0;

        for (value, typ) in values.iter().zip(self.header.row_types()) {
            value.write_to(&mut self.buf[prev_end..prev_end + typ.size()]);
            prev_end += typ.size();
        }

        self.writer.write_all(&self.buf).unwrap();
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.writer.flush()?;

        match self.writer.into_inner() {
            Ok(w) => Ok(w),
            Err(_) => unreachable!("Failed to unwrap writer into inner value"),
        }
    }
}

pub struct RowIndex {
    header: Header,
    index: Vec<Option<usize>>,
    body: OwnedBytes,
}

impl RowIndex {
    pub fn open(bytes: OwnedBytes) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self {
                header: Header {
                    field_order: vec![],
                    row_size_bytes: 0,
                },
                index: vec![],
                body: bytes,
            });
        }

        let num_header_bytes = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
        let serialized_header = &bytes[8..8 + num_header_bytes];
        let header = Header::deserialize(serialized_header)?;
        let num_bytes = bytes.len();
        let body = bytes.slice(8 + num_header_bytes..num_bytes);

        let index = header.row_index();

        Ok(Self {
            header,
            index,
            body,
        })
    }

    pub fn get_row(&self, idx: usize) -> Option<Row> {
        if self.header.field_order().is_empty() {
            return None;
        }

        let start = idx * self.header.row_size_bytes();
        let end = start + self.header.row_size_bytes();

        if end > self.body.len() {
            return None;
        }

        let value_bytes = self.body.slice(start..end);
        let mut values = Vec::new();
        let mut offset = 0;

        for field in self.header.field_order().iter() {
            let typ = field.row_value_type();

            if offset + typ.size() > value_bytes.len() {
                return None;
            }

            let value = RowValue::read_from(&value_bytes[offset..offset + typ.size()], typ);
            values.push(value);
            offset += typ.size();
        }

        Some(Row {
            index: self.index.clone(),
            values,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = Row> + '_ {
        (0..self.num_rows()).filter_map(move |idx| self.get_row(idx))
    }

    pub fn num_rows(&self) -> usize {
        if self.body.is_empty() {
            return 0;
        }

        debug_assert_eq!(
            self.body.len() % self.header.row_size_bytes(),
            0,
            "Length of body is not a multiple of row size"
        );

        self.body.len() / self.header.row_size_bytes()
    }

    pub fn field_by_id(&self, id: u32) -> Option<&Field> {
        let idx = (*self.index.get(id as usize)?)?;
        self.header.field_order.get(idx)
    }

    pub fn total_num_bytes_for_field(&self, field: &Field) -> usize {
        field.row_value_type().size() * self.num_rows()
    }

    fn header(&self) -> &Header {
        &self.header
    }
}

#[derive(Debug, Clone)]
pub struct MergeAddr {
    pub segment_ord: usize,
}

#[derive(Debug, Clone)]
pub enum MergeRowOrder {
    Stack,
    Shuffled { addrs: Vec<MergeAddr> },
}

pub fn merge<I, W>(indexes: &[I], order: MergeRowOrder, writer: W) -> Result<W>
where
    W: Write,
    I: Borrow<RowIndex>,
{
    if indexes.is_empty() {
        return Ok(writer);
    }

    let first = indexes[0].borrow();
    debug_assert!(
        indexes
            .iter()
            .all(|index| index.borrow().header().field_order() == first.header().field_order()),
        "Indexes have different field orders"
    );

    debug_assert!(
        indexes.iter().all(
            |index| index.borrow().header().row_size_bytes() == first.header().row_size_bytes()
        ),
        "Indexes have different row sizes"
    );

    let mut writer = RowIndexer::new(writer, first.header().field_order().to_vec())?;

    match order {
        MergeRowOrder::Stack => {
            for index in indexes {
                for row in index.borrow().iter() {
                    writer.write_row(row.values.as_slice());
                }
            }
        }
        MergeRowOrder::Shuffled { addrs } => {
            let mut iters: Vec<_> = indexes.iter().map(|index| index.borrow().iter()).collect();

            for addr in addrs {
                if let Some(row) = iters[addr.segment_ord].next() {
                    writer.write_row(row.values.as_slice())
                }
            }
        }
    }

    Ok(writer.finish()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_value_ser() {
        let mut buf = [0u8; 8];
        let value = RowValue::U64(42);
        value.write_to(&mut buf);
        assert_eq!(RowValue::read_from(&buf, RowValueType::U64), value);

        let value = RowValue::F64(42.0);
        value.write_to(&mut buf);
        assert_eq!(RowValue::read_from(&buf, RowValueType::F64), value);
    }

    #[test]
    fn test_field_ser() {
        let field = Field::new("test".to_string(), 42, RowValueType::U64);
        let mut buf = Vec::new();
        field.write_to(&mut buf);
        let (deserialized, _) = Field::read_from(&buf).unwrap();
        assert_eq!(field, deserialized);
    }

    #[test]
    fn test_row_index() {
        let buf = Vec::new();

        let mut indexer = RowIndexer::new(
            buf,
            vec![
                Field::new("test".to_string(), 0, RowValueType::U64),
                Field::new("test2".to_string(), 1, RowValueType::F64),
            ],
        )
        .unwrap();

        indexer.write_row(&[RowValue::U64(0), RowValue::F64(0.0)]);
        indexer.write_row(&[RowValue::U64(1), RowValue::F64(0.0)]);
        indexer.write_row(&[RowValue::U64(42), RowValue::F64(42.0)]);

        let bytes = indexer.finish().unwrap();
        let index = RowIndex::open(bytes.into()).unwrap();

        let row = index.get_row(0).unwrap();
        assert_eq!(row.get_by_field_id(0), Some(&RowValue::U64(0)));
        assert_eq!(row.get_by_field_id(1), Some(&RowValue::F64(0.0)));

        let row = index.get_row(1).unwrap();
        assert_eq!(row.get_by_field_id(0), Some(&RowValue::U64(1)));
        assert_eq!(row.get_by_field_id(1), Some(&RowValue::F64(0.0)));

        let row = index.get_row(2).unwrap();
        assert_eq!(row.get_by_field_id(0), Some(&RowValue::U64(42)));
        assert_eq!(row.get_by_field_id(1), Some(&RowValue::F64(42.0)));

        assert!(index.get_row(3).is_none());
    }

    #[test]
    fn test_empty_row_index() {
        let buf = Vec::new();

        let indexer = RowIndexer::new(buf, vec![]).unwrap();
        let bytes = indexer.finish().unwrap();
        let index = RowIndex::open(bytes.into()).unwrap();

        assert!(index.get_row(0).is_none());

        let buf = Vec::new();
        let indexer = RowIndexer::new(
            buf,
            vec![Field::new("test".to_string(), 0, RowValueType::U64)],
        )
        .unwrap();
        let bytes = indexer.finish().unwrap();
        let index = RowIndex::open(bytes.into()).unwrap();

        assert!(index.get_row(0).is_none());
    }

    #[test]
    fn test_iter() {
        let buf = Vec::new();

        let mut indexer = RowIndexer::new(
            buf,
            vec![
                Field::new("a".to_string(), 0, RowValueType::U64),
                Field::new("b".to_string(), 1, RowValueType::F64),
            ],
        )
        .unwrap();

        let rows = [
            vec![RowValue::U64(0), RowValue::F64(0.0)],
            vec![RowValue::U64(1), RowValue::F64(0.0)],
            vec![RowValue::U64(42), RowValue::F64(42.0)],
        ];

        for row in rows.iter() {
            indexer.write_row(row);
        }

        let bytes = indexer.finish().unwrap();
        let index = RowIndex::open(bytes.into()).unwrap();

        let stored_rows: Vec<_> = index.iter().collect();
        assert_eq!(rows.len(), stored_rows.len());

        for (expected, stored) in rows.iter().zip(stored_rows.iter()) {
            assert_eq!(expected, &stored.values);
        }
    }

    #[test]
    fn test_empty_iter() {
        let buf = Vec::new();

        let indexer = RowIndexer::new(buf, vec![]).unwrap();
        let bytes = indexer.finish().unwrap();
        let index = RowIndex::open(bytes.into()).unwrap();

        let rows: Vec<_> = index.iter().collect();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_merge_stacked() {
        let mut indexers = Vec::new();

        for _ in 0..3 {
            let buf = Vec::new();
            let indexer = RowIndexer::new(
                buf,
                vec![
                    Field::new("a".to_string(), 0, RowValueType::U64),
                    Field::new("b".to_string(), 1, RowValueType::F64),
                ],
            )
            .unwrap();

            indexers.push(indexer);
        }

        let rows = [
            vec![RowValue::U64(0), RowValue::F64(0.0)],
            vec![RowValue::U64(1), RowValue::F64(0.0)],
            vec![RowValue::U64(42), RowValue::F64(42.0)],
        ];

        let mut indexes = Vec::new();
        for mut indexer in indexers {
            for row in rows.iter() {
                indexer.write_row(row);
            }

            let bytes = indexer.finish().unwrap();
            let index = RowIndex::open(bytes.into()).unwrap();
            indexes.push(index);
        }

        let buf = Vec::new();
        let buf = merge(&indexes, MergeRowOrder::Stack, buf).unwrap();
        let index = RowIndex::open(buf.into()).unwrap();

        let stored_rows: Vec<_> = index.iter().collect();
        assert_eq!(rows.len() * indexes.len(), stored_rows.len());

        for (expected, stored) in rows
            .iter()
            .cycle()
            .take(stored_rows.len())
            .zip(stored_rows.iter())
        {
            assert_eq!(expected, &stored.values);
        }
    }

    #[test]
    fn test_merge_shuffled() {
        let mut indexers = Vec::new();

        for _ in 0..3 {
            let buf = Vec::new();
            let indexer = RowIndexer::new(
                buf,
                vec![
                    Field::new("a".to_string(), 0, RowValueType::U64),
                    Field::new("b".to_string(), 1, RowValueType::F64),
                ],
            )
            .unwrap();

            indexers.push(indexer);
        }

        let rows = [
            vec![RowValue::U64(0), RowValue::F64(0.0)],
            vec![RowValue::U64(1), RowValue::F64(0.0)],
            vec![RowValue::U64(42), RowValue::F64(42.0)],
        ];

        let mut indexes = Vec::new();
        for mut indexer in indexers {
            for row in rows.iter() {
                indexer.write_row(row);
            }

            let bytes = indexer.finish().unwrap();
            let index = RowIndex::open(bytes.into()).unwrap();
            indexes.push(index);
        }

        let buf = Vec::new();
        let buf = merge(
            &indexes,
            MergeRowOrder::Shuffled {
                addrs: (0..indexes.len())
                    .map(|i| MergeAddr { segment_ord: i })
                    .cycle()
                    .take(indexes.len() * rows.len())
                    .collect(),
            },
            buf,
        )
        .unwrap();
        let index = RowIndex::open(buf.into()).unwrap();

        let stored_rows: Vec<_> = index.iter().collect();
        assert_eq!(rows.len() * indexes.len(), stored_rows.len());

        stored_rows
            .chunks(indexes.len())
            .enumerate()
            .for_each(|(i, chunk)| {
                let row = &rows[i];

                for c in chunk {
                    assert_eq!(row, &c.values);
                }
            });
    }

    #[test]
    fn test_merge_shuffled_diff_len() {
        let mut indexers = Vec::new();

        for _ in 0..2 {
            let buf = Vec::new();
            let indexer = RowIndexer::new(
                buf,
                vec![
                    Field::new("a".to_string(), 0, RowValueType::U64),
                    Field::new("b".to_string(), 1, RowValueType::F64),
                ],
            )
            .unwrap();

            indexers.push(indexer);
        }

        indexers[0].write_row(&[RowValue::U64(0), RowValue::F64(0.0)]);
        indexers[0].write_row(&[RowValue::U64(1), RowValue::F64(0.0)]);
        indexers[1].write_row(&[RowValue::U64(42), RowValue::F64(42.0)]);

        let mut indexes = Vec::new();
        for indexer in indexers {
            let bytes = indexer.finish().unwrap();
            let index = RowIndex::open(bytes.into()).unwrap();
            indexes.push(index);
        }

        let order = MergeRowOrder::Shuffled {
            addrs: vec![
                MergeAddr { segment_ord: 0 },
                MergeAddr { segment_ord: 1 },
                MergeAddr { segment_ord: 0 },
            ],
        };
        let buf = Vec::new();

        let buf = merge(&indexes, order, buf).unwrap();
        let index = RowIndex::open(buf.into()).unwrap();

        let stored_rows: Vec<_> = index.iter().collect();
        assert_eq!(stored_rows.len(), 3);

        let rows = [
            vec![RowValue::U64(0), RowValue::F64(0.0)],
            vec![RowValue::U64(42), RowValue::F64(42.0)],
            vec![RowValue::U64(1), RowValue::F64(0.0)],
        ];

        for (expected, stored) in rows.iter().zip(stored_rows.iter()) {
            assert_eq!(expected, &stored.values);
        }
    }

    #[test]
    fn test_merge_empty() {
        let buf = Vec::new();
        let buf = merge(&[] as &[RowIndex], MergeRowOrder::Stack, buf).unwrap();
        let index = RowIndex::open(buf.into()).unwrap();

        assert_eq!(index.num_rows(), 0);

        let buf = Vec::new();
        let buf = merge(
            &[] as &[RowIndex],
            MergeRowOrder::Shuffled { addrs: vec![] },
            buf,
        )
        .unwrap();
        let index = RowIndex::open(buf.into()).unwrap();

        assert_eq!(index.num_rows(), 0);

        let a = RowIndexer::new(Vec::new(), vec![])
            .unwrap()
            .finish()
            .unwrap();
        let b = RowIndexer::new(Vec::new(), vec![])
            .unwrap()
            .finish()
            .unwrap();

        let buf = merge(
            &[
                RowIndex::open(a.into()).unwrap(),
                RowIndex::open(b.into()).unwrap(),
            ],
            MergeRowOrder::Stack,
            Vec::new(),
        )
        .unwrap();

        let index = RowIndex::open(buf.into()).unwrap();
        assert_eq!(index.num_rows(), 0);

        let a = RowIndexer::new(
            Vec::new(),
            vec![
                Field::new("a".to_string(), 0, RowValueType::U64),
                Field::new("b".to_string(), 1, RowValueType::F64),
            ],
        )
        .unwrap()
        .finish()
        .unwrap();

        let b = RowIndexer::new(
            Vec::new(),
            vec![
                Field::new("a".to_string(), 0, RowValueType::U64),
                Field::new("b".to_string(), 1, RowValueType::F64),
            ],
        )
        .unwrap()
        .finish()
        .unwrap();

        let buf = merge(
            &[
                RowIndex::open(a.into()).unwrap(),
                RowIndex::open(b.into()).unwrap(),
            ],
            MergeRowOrder::Stack,
            Vec::new(),
        )
        .unwrap();

        let index = RowIndex::open(buf.into()).unwrap();
        assert_eq!(index.num_rows(), 0);
    }
}
