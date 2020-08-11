use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashSet, HashMap, LinkedList};
use std::hash::{Hash, Hasher};

use bytes::{BufMut, Buf, BytesMut, Bytes};
use uuid::Uuid;
use linked_hash_set::LinkedHashSet;
use linked_hash_map::LinkedHashMap;
use chrono::{NaiveDateTime, Timelike};

use crate::error::{Result, ErrorKind, Error};
use crate::network::Tcp;

const PROTO_VER: i8 = 1;

pub struct Binary {
    tcp: Rc<RefCell<Tcp>>,
}

impl Binary {
    pub(crate) fn new(tcp: Rc<RefCell<Tcp>>) -> Binary {
        Binary { tcp }
    }

    pub fn type_name(&self, type_id: i32) -> Result<Option<String>> {
        self.tcp.borrow_mut().execute(
            3000,
            |request| {
                0i8.write(request)?;
                type_id.write(request)?;

                Ok(())
            },
            |response| {
                <Option<String>>::read(response)
            }
        )
    }

    pub fn register_type_name(&self, type_id: i32, type_name: &str) -> Result<()> {
        self.tcp.borrow_mut().execute(
            3001,
            |request| {
                0i8.write(request)?;
                type_id.write(request)?;
                type_name.to_string().write(request)?;

                Ok(())
            },
            |_| { Ok(()) }
        )
    }

    pub fn get_type(&self, type_id: i32) -> Result<Option<Type>> {
        self.tcp.borrow_mut().execute(
            3002,
            |request| {
                type_id.write(request)?;

                Ok(())
            },
            |response| {
                Ok(
                    if bool::read(response)? {
                        Some(Type::read(response)?)
                    }
                    else {
                        None
                    }
                )
            }
        )
    }

    pub fn put_type(&self, type_desc: Type) -> Result<()> {
        self.tcp.borrow_mut().execute(
            3003,
            |request| {
                type_desc.write(request)
            },
            |_| { Ok(()) }
        )
    }
}

pub struct Type {
    pub id: i32,
    pub name: String,
    pub affinity_key_field_name: String,
    pub fields: Vec<Field>,
    pub enum_fields: Option<Vec<(String, i32)>>,
    pub schemas: Vec<Schema>,
}

impl IgniteRead for Type {
    fn read(bytes: &mut Bytes) -> Result<Self> {
        let id = i32::read(bytes)?;
        let name = String::read(bytes)?;
        let affinity_key_field_name = String::read(bytes)?;
        let fields = <Vec<Field>>::read(bytes)?;
        let enum_fields =
            if bool::read(bytes)? {
                Some(<Vec<(String, i32)>>::read(bytes)?)
            }
            else {
                None
            };
        let schemas = <Vec<Schema>>::read(bytes)?;

        Ok(Type {
            id,
            name,
            affinity_key_field_name,
            fields,
            enum_fields,
            schemas,
        })
    }
}

impl IgniteWrite for Type {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        self.id.write(bytes)?;
        self.name.write(bytes)?;
        self.affinity_key_field_name.write(bytes)?;
        self.fields.write(bytes)?;

        match &self.enum_fields {
            Some(enum_fields) => {
                true.write(bytes)?;
                enum_fields.write(bytes)?;
            },
            None => {
                false.write(bytes)?;
            },
        }

        self.schemas.write(bytes)?;

        Ok(())
    }
}

#[derive(IgniteRead, IgniteWrite)]
pub struct Field {
    pub name: String,
    pub type_id: i32,
    pub field_id: i32,
}

#[derive(IgniteRead, IgniteWrite)]
pub struct Schema {
    pub id: i32,
    pub fields: Vec<(i32, i32)>,
}

#[derive(Debug)]
pub enum Value {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Char(char),
    Bool(bool),
    String(String),
    Uuid(Uuid),
    Timestamp(NaiveDateTime),
    I8Vec(Vec<i8>),
    I16Vec(Vec<i16>),
    I32Vec(Vec<i32>),
    I64Vec(Vec<i64>),
    F32Vec(Vec<f32>),
    F64Vec(Vec<f64>),
    CharVec(Vec<char>),
    BoolVec(Vec<bool>),
    StringVec(Vec<String>),
    UuidVec(Vec<Uuid>),
    TimestampVec(Vec<NaiveDateTime>),
    Vec(Vec<Value>),
    LinkedList(LinkedList<Value>),
    HashSet(HashSet<Value>),
    LinkedHashSet(LinkedHashSet<Value>),
    HashMap(HashMap<Value, Value>),
    LinkedHashMap(LinkedHashMap<Value, Value>),
    BinaryObject(BinaryObject),
}

// TODO: Implement
impl PartialEq for Value {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }

    fn ne(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

// TODO: Eq vs PartialEq?
impl Eq for Value {}

// TODO: Implement
impl Hash for Value {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        unimplemented!()
    }

    fn hash_slice<H: Hasher>(_data: &[Self], _state: &mut H)
        where Self: Sized
    {
        unimplemented!()
    }
}

#[derive(PartialEq, Debug)]
pub struct BinaryObject {
    flags: i16,
    type_id: i32,
    hash_code: i32,
    bytes: Bytes,
}

impl BinaryObject {
    pub fn field(&self, _name: &str) -> Result<Option<Value>> {
        Ok(None)
    }
}

pub(crate) trait Nullable {}

impl Nullable for Value {}
impl Nullable for String {}
impl Nullable for Uuid {}
impl Nullable for NaiveDateTime {}

pub(crate) trait IgniteWrite {
    fn write(&self, bytes: &mut BytesMut) -> Result<()>;
}

macro_rules! write_collection {
    ($bytes:expr, $col:expr, $type:expr) => {
        $bytes.put_i8(24);
        $bytes.put_i32_le($col.len() as i32);
        $bytes.put_i8($type);

        for item in $col {
            item.write($bytes)?;
        }
    }
}

macro_rules! write_map {
    ($bytes:expr, $col:expr, $type:expr) => {
        $bytes.put_i8(25);
        $bytes.put_i32_le($col.len() as i32);
        $bytes.put_i8($type);

        for (k, v) in $col {
            k.write($bytes)?;
            v.write($bytes)?;
        }
    }
}

impl IgniteWrite for Value {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        match self {
            Value::I8(v) => {
                bytes.put_i8(1);

                v.write(bytes)
            },
            Value::I16(v) => {
                bytes.put_i8(2);

                v.write(bytes)
            },
            Value::I32(v) => {
                bytes.put_i8(3);

                v.write(bytes)
            },
            Value::I64(v) => {
                bytes.put_i8(4);

                v.write(bytes)
            },
            Value::F32(v) => {
                bytes.put_i8(5);

                v.write(bytes)
            },
            Value::F64(v) => {
                bytes.put_i8(6);

                v.write(bytes)
            },
            Value::Char(v) => {
                bytes.put_i8(7);

                v.write(bytes)
            },
            Value::Bool(v) => {
                bytes.put_i8(8);

                v.write(bytes)
            },
            Value::String(v) => {
                v.write(bytes)
            },
            Value::Uuid(v) => {
                v.write(bytes)
            },
            Value::Timestamp(v) => {
                v.write(bytes)
            }
            Value::I8Vec(v) => {
                bytes.put_i8(12);

                v.write(bytes)
            },
            Value::I16Vec(v) => {
                bytes.put_i8(13);

                v.write(bytes)
            },
            Value::I32Vec(v) => {
                bytes.put_i8(14);

                v.write(bytes)
            },
            Value::I64Vec(v) => {
                bytes.put_i8(15);

                v.write(bytes)
            },
            Value::F32Vec(v) => {
                bytes.put_i8(16);

                v.write(bytes)
            },
            Value::F64Vec(v) => {
                bytes.put_i8(17);

                v.write(bytes)
            },
            Value::CharVec(v) => {
                bytes.put_i8(18);

                v.write(bytes)
            },
            Value::BoolVec(v) => {
                bytes.put_i8(19);

                v.write(bytes)
            },
            Value::StringVec(v) => {
                bytes.put_i8(20);

                v.write(bytes)
            },
            Value::UuidVec(v) => {
                bytes.put_i8(21);

                v.write(bytes)
            },
            Value::TimestampVec(v) => {
                bytes.put_i8(34);

                v.write(bytes)
            },
            Value::Vec(v) => {
                write_collection!(bytes, v, 1);

                Ok(())
            },
            Value::LinkedList(v) => {
                write_collection!(bytes, v, 2);

                Ok(())
            },
            Value::HashSet(v) => {
                write_collection!(bytes, v, 3);

                Ok(())
            },
            Value::LinkedHashSet(v) => {
                write_collection!(bytes, v, 4);

                Ok(())
            },
            Value::HashMap(v) => {
                write_map!(bytes, v, 1);

                Ok(())
            },
            Value::LinkedHashMap(v) => {
                write_map!(bytes, v, 2);

                Ok(())
            },
            Value::BinaryObject(v) => {
                bytes.put_i8(103);
                bytes.put_i8(PROTO_VER);
                bytes.put_i16_le(v.flags);
                bytes.put_i32_le(v.type_id);
                bytes.put_i32_le(v.hash_code);
                bytes.put_i32_le((v.bytes.len() + 16) as i32);
                bytes.put(v.bytes.clone()); // TODO: Can we get rid of clone?

                Ok(())
            },
        }
    }
}

impl IgniteWrite for i8 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(*self);

        Ok(())
    }
}

impl IgniteWrite for i16 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i16_le(*self);

        Ok(())
    }
}

impl IgniteWrite for i32 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i32_le(*self);

        Ok(())
    }
}

impl IgniteWrite for i64 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i64_le(*self);

        Ok(())
    }
}

impl IgniteWrite for f32 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_f32_le(*self);

        Ok(())
    }
}

impl IgniteWrite for f64 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_f64_le(*self);

        Ok(())
    }
}

impl IgniteWrite for char {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        if self.len_utf16() == 1 {
            bytes.put_u16(*self as u16);

            Ok(())
        }
        else {
            Err(Error::new(ErrorKind::Serde, "Only UTF-16 characters are supported.".to_string()))
        }
    }
}

impl IgniteWrite for bool {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_u8(if *self { 1 } else { 0 });

        Ok(())
    }
}

impl IgniteWrite for String {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        let arr = self.as_bytes();

        bytes.put_i8(9);
        bytes.put_i32_le(arr.len() as i32);
        bytes.put_slice(arr);

        Ok(())
    }
}

impl IgniteWrite for Uuid {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        let arr = self.as_bytes();

        let mut msb: i64 = 0;
        let mut lsb: i64 = 0;

        for i in 0 .. 8 {
            msb = (msb << 8) | (arr[i] as i64 & 0xFF);
        }

        for i in 8 .. 16 {
            lsb = (lsb << 8) | (arr[i] as i64 & 0xFF);
        }

        bytes.put_i8(10);
        bytes.put_i64_le(msb);
        bytes.put_i64_le(lsb);

        Ok(())
    }
}

impl IgniteWrite for NaiveDateTime {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(33);
        bytes.put_i64_le(self.timestamp_millis());
        bytes.put_i32_le(self.nanosecond() as i32);

        Ok(())
    }
}

impl<T: IgniteWrite + Nullable> IgniteWrite for Option<T> {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        match self {
            Some(value) => {
                value.write(bytes)
            },
            None => {
                bytes.put_i8(101);

                Ok(())
            },
        }
    }
}

impl<T: IgniteWrite> IgniteWrite for Vec<T> {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i32_le(self.len() as i32);

        for item in self {
            item.write(bytes)?;
        }

        Ok(())
    }
}

impl<T: IgniteWrite> IgniteWrite for &[T] {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i32_le(self.len() as i32);

        for item in self.iter() {
            item.write(bytes)?;
        }

        Ok(())
    }
}

impl<T1: IgniteWrite, T2: IgniteWrite> IgniteWrite for (T1, T2) {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        let (v1, v2) = self;

        v1.write(bytes)?;
        v2.write(bytes)?;

        Ok(())
    }
}

pub(crate) trait IgniteRead: Sized {
    fn read(bytes: &mut Bytes) -> Result<Self>;
}

impl IgniteRead for Value {
    fn read(bytes: &mut Bytes) -> Result<Value> {
        let type_code = *bytes.first()
            .ok_or_else(|| Error::new(ErrorKind::Serde, "Out of bytes.".to_string()))?;

        if type_code >= 1 && type_code <= 8 {
            bytes.advance(1);
        }

        match type_code {
            1 => Ok(Value::I8(i8::read(bytes)?)),
            2 => Ok(Value::I16(i16::read(bytes)?)),
            3 => Ok(Value::I32(i32::read(bytes)?)),
            4 => Ok(Value::I64(i64::read(bytes)?)),
            5 => Ok(Value::F32(f32::read(bytes)?)),
            6 => Ok(Value::F64(f64::read(bytes)?)),
            7 => Ok(Value::Char(char::read(bytes)?)),
            8 => Ok(Value::Bool(bool::read(bytes)?)),
            9 => Ok(Value::String(String::read(bytes)?)),
            10 => Ok(Value::Uuid(Uuid::read(bytes)?)),
            33 => Ok(Value::Timestamp(NaiveDateTime::read(bytes)?)),
            12 => Ok(Value::I8Vec(<Vec<i8>>::read(bytes)?)),
            13 => Ok(Value::I16Vec(<Vec<i16>>::read(bytes)?)),
            14 => Ok(Value::I32Vec(<Vec<i32>>::read(bytes)?)),
            15 => Ok(Value::I64Vec(<Vec<i64>>::read(bytes)?)),
            16 => Ok(Value::F32Vec(<Vec<f32>>::read(bytes)?)),
            17 => Ok(Value::F64Vec(<Vec<f64>>::read(bytes)?)),
            18 => Ok(Value::CharVec(<Vec<char>>::read(bytes)?)),
            19 => Ok(Value::BoolVec(<Vec<bool>>::read(bytes)?)),
            20 => Ok(Value::StringVec(<Vec<String>>::read(bytes)?)),
            21 => Ok(Value::UuidVec(<Vec<Uuid>>::read(bytes)?)),
            34 => Ok(Value::TimestampVec(<Vec<NaiveDateTime>>::read(bytes)?)),
            24 => {
                let len = bytes.get_i32_le() as usize;
                let col_type = bytes.get_i8();

                match col_type {
                    -1 | 0 | 1 | 5 => {
                        let mut vec = Vec::with_capacity(len);

                        for _ in 0 .. len {
                            vec.push(Value::read(bytes)?);
                        }

                        Ok(Value::Vec(vec))
                    },
                    2 => {
                        let mut linked_list = LinkedList::new();

                        for _ in 0 .. len {
                            linked_list.push_back(Value::read(bytes)?);
                        }

                        Ok(Value::LinkedList(linked_list))
                    },
                    3 => {
                        let mut hash_set = HashSet::with_capacity(len);

                        for _ in 0 .. len {
                            hash_set.insert(Value::read(bytes)?);
                        }

                        Ok(Value::HashSet(hash_set))
                    },
                    4 => {
                        let mut linked_hash_set = LinkedHashSet::with_capacity(len);

                        for _ in 0 .. len {
                            linked_hash_set.insert(Value::read(bytes)?);
                        }

                        Ok(Value::LinkedHashSet(linked_hash_set))
                    },
                    _ => Err(Error::new(ErrorKind::Serde, format!("Invalid collection type: {}", col_type))),
                }
            },
            25 => {
                let len = bytes.get_i32_le() as usize;
                let map_type = bytes.get_i8();

                match map_type {
                    1 => {
                        let mut hash_map = HashMap::with_capacity(len);

                        for _ in 0 .. len {
                            hash_map.insert(Value::read(bytes)?, Value::read(bytes)?);
                        }

                        Ok(Value::HashMap(hash_map))
                    },
                    2 => {
                        let mut linked_hash_map = LinkedHashMap::with_capacity(len);

                        for _ in 0 .. len {
                            linked_hash_map.insert(Value::read(bytes)?, Value::read(bytes)?);
                        }

                        Ok(Value::LinkedHashMap(linked_hash_map))
                    },
                    _ => Err(Error::new(ErrorKind::Serde, format!("Invalid map type: {}", map_type))),
                }
            },
            103 => {
                let proto_ver = bytes.get_i8();

                if proto_ver == PROTO_VER {
                    let flags = bytes.get_i16_le();
                    let type_id = bytes.get_i32_le();
                    let hash_code = bytes.get_i32_le();
                    let len = (bytes.get_i32_le() - 16) as usize;

                    Ok(Value::BinaryObject(BinaryObject {
                        flags,
                        type_id,
                        hash_code,
                        bytes: bytes.slice(..len),
                    }))
                }
                else {
                    Err(Error::new(ErrorKind::Serde, format!("Unsupported protocol version: {}", proto_ver)))
                }
            },
            _ => Err(Error::new(ErrorKind::Serde, format!("Invalid type code: {}", type_code))),
        }
    }
}

impl IgniteRead for i8 {
    fn read(bytes: &mut Bytes) -> Result<i8> {
        Ok(bytes.get_i8())
    }
}

impl IgniteRead for i16 {
    fn read(bytes: &mut Bytes) -> Result<i16> {
        Ok(bytes.get_i16_le())
    }
}

impl IgniteRead for i32 {
    fn read(bytes: &mut Bytes) -> Result<i32> {
        Ok(bytes.get_i32_le())
    }
}

impl IgniteRead for i64 {
    fn read(bytes: &mut Bytes) -> Result<i64> {
        Ok(bytes.get_i64_le())
    }
}

impl IgniteRead for f32 {
    fn read(bytes: &mut Bytes) -> Result<f32> {
        Ok(bytes.get_f32_le())
    }
}

impl IgniteRead for f64 {
    fn read(bytes: &mut Bytes) -> Result<f64> {
        Ok(bytes.get_f64_le())
    }
}

impl IgniteRead for char {
    fn read(bytes: &mut Bytes) -> Result<char> {
        let value = bytes.get_u16_le();

        if let Some(char) = std::char::from_u32(value as u32) {
            Ok(char)
        }
        else {
            Err(Error::new(ErrorKind::Serde, format!("Failed to convert to char: {}", value)))
        }
    }
}

impl IgniteRead for bool {
    fn read(bytes: &mut Bytes) -> Result<bool> {
        Ok(bytes.get_u8() != 0)
    }
}

impl IgniteRead for String {
    fn read(bytes: &mut Bytes) -> Result<String> {
        check_flag(bytes, 9)?;

        let len = bytes.get_i32_le() as usize;

        let vec = bytes.slice(..len).to_vec();

        bytes.advance(len);

        Ok(String::from_utf8(vec)?)
    }
}

impl IgniteRead for Uuid {
    fn read(bytes: &mut Bytes) -> Result<Uuid> {
        check_flag(bytes, 10)?;

        let mut msb = bytes.get_i64_le();
        let mut lsb = bytes.get_i64_le();

        let mut arr = [0u8; 16];

        for i in 0 .. 8 {
            arr[15 - i] = (lsb & 0xFF) as u8;

            lsb = lsb >> 8;
        }

        for i in 8 .. 16 {
            arr[15 - i] = (msb & 0xFF) as u8;

            msb = msb >> 8;
        }

        Ok(Uuid::from_bytes(arr))
    }
}

impl IgniteRead for NaiveDateTime {
    fn read(bytes: &mut Bytes) -> Result<NaiveDateTime> {
        check_flag(bytes, 33)?;

        let millis = bytes.get_i64_le();
        let nanos = bytes.get_i32_le() as u32;

        // TODO: Expects seconds?
        Ok(NaiveDateTime::from_timestamp(millis, nanos))
    }
}

impl<T: IgniteRead + Nullable> IgniteRead for Option<T> {
    fn read(bytes: &mut Bytes) -> Result<Option<T>> {
        let flag = bytes.first();

        match flag {
            None => Err(Error::new(ErrorKind::Serde, "Out of bytes".to_string())),
            Some(101) => {
                bytes.advance(1);

                Ok(None)
            },
            _ => Ok(Some(T::read(bytes)?))
        }
    }
}

impl<T: IgniteRead> IgniteRead for Vec<T> {
    fn read(bytes: &mut Bytes) -> Result<Self> {
        let len = bytes.get_i32_le() as usize;

        let mut vec = Vec::with_capacity(len);

        for _ in 0 .. len {
            vec.push(T::read(bytes)?);
        }

        Ok(vec)
    }
}

impl<T1: IgniteRead, T2: IgniteRead> IgniteRead for (T1, T2) {
    fn read(bytes: &mut Bytes) -> Result<(T1, T2)> {
        let v1 = T1::read(bytes)?;
        let v2 = T2::read(bytes)?;

        Ok((v1, v2))
    }
}

fn check_flag(bytes: &mut Bytes, expected: i8) -> Result<()> {
    let flag = bytes.get_i8();

    if flag == expected {
        Ok(())
    }
    else {
        Err(Error::new(ErrorKind::Serde, format!("Unexpected flag: {} != {}", flag, expected)))
    }
}
