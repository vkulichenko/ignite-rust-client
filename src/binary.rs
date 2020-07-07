use bytes::{BufMut, Buf, BytesMut, Bytes};
use uuid::Uuid;

use crate::error::{Result, ErrorKind, Error};

#[derive(PartialEq, Debug)]
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
}

impl Value {
    pub(crate) fn read(bytes: &mut Bytes) -> Result<Option<Value>> {
        let type_code = bytes.get_i8();

        match type_code {
            101 => Ok(None),
            1 => Ok(Some(Value::I8(bytes.get_i8()))),
            2 => Ok(Some(Value::I16(bytes.get_i16_le()))),
            3 => Ok(Some(Value::I32(bytes.get_i32_le()))),
            4 => Ok(Some(Value::I64(bytes.get_i64_le()))),
            5 => Ok(Some(Value::F32(bytes.get_f32_le()))),
            6 => Ok(Some(Value::F64(bytes.get_f64_le()))),
            7 => Ok(Some(Value::Char(read_char(bytes)?))),
            8 => Ok(Some(Value::Bool(read_bool(bytes)))),
            9 => Ok(Some(Value::String(read_string(bytes)?))),
            10 => Ok(Some(Value::Uuid(read_uuid(bytes)))),
            _ => Err(Error::new(ErrorKind::Ignite(0), format!("Invalid type code: {}", type_code))),
        }
    }
}

pub(crate) trait BinaryWrite {
    fn write(&self, bytes: &mut BytesMut) -> Result<()>;
}

impl BinaryWrite for Value {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        match self {
            Value::I8(v) => v.write(bytes),
            Value::I16(v) => v.write(bytes),
            Value::I32(v) => v.write(bytes),
            Value::I64(v) => v.write(bytes),
            Value::F32(v) => v.write(bytes),
            Value::F64(v) => v.write(bytes),
            Value::Char(v) => v.write(bytes),
            Value::Bool(v) => v.write(bytes),
            Value::String(v) => v.write(bytes),
            Value::Uuid(v) => v.write(bytes),
        }
    }
}

impl BinaryWrite for i8 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(1);
        bytes.put_i8(*self);

        Ok(())
    }
}

impl BinaryWrite for i16 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(2);
        bytes.put_i16_le(*self);

        Ok(())
    }
}

impl BinaryWrite for i32 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(3);
        bytes.put_i32_le(*self);

        Ok(())
    }
}

impl BinaryWrite for i64 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(4);
        bytes.put_i64_le(*self);

        Ok(())
    }
}

impl BinaryWrite for f32 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(5);
        bytes.put_f32_le(*self);

        Ok(())
    }
}

impl BinaryWrite for f64 {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(6);
        bytes.put_f64_le(*self);

        Ok(())
    }
}

impl BinaryWrite for char {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        if self.len_utf16() == 1 {
            bytes.put_i8(7);
            bytes.put_u16(*self as u16);

            Ok(())
        }
        else {
            Err(Error::new(ErrorKind::Serde, "Only UTF-16 characters are supported.".to_string()))
        }
    }
}

impl BinaryWrite for bool {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        bytes.put_i8(8);
        bytes.put_u8(if *self { 1 } else { 0 });

        Ok(())
    }
}

impl BinaryWrite for String {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        let arr = self.as_bytes();

        bytes.put_i8(9);
        bytes.put_i32_le(arr.len() as i32);
        bytes.put_slice(arr);

        Ok(())
    }
}

impl BinaryWrite for Uuid {
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

impl<T> BinaryWrite for Option<T>
    where T: BinaryWrite
{
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

fn read_char(bytes: &mut Bytes) -> Result<char> {
    let value = bytes.get_u16_le();

    if let Some(char) = std::char::from_u32(value as u32) {
        Ok(char)
    }
    else {
        Err(Error::new(ErrorKind::Serde, format!("Failed to convert to char: {}", value)))
    }
}

fn read_bool(bytes: &mut Bytes) -> bool {
    bytes.get_u8() != 0
}

fn read_string(bytes: &mut Bytes) -> Result<String> {
    let len = bytes.get_i32_le() as usize;

    let vec = bytes.slice(..len).to_vec();

    bytes.advance(len);

    Ok(String::from_utf8(vec)?)
}

fn read_uuid(bytes: &mut Bytes) -> Uuid {
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

    Uuid::from_bytes(arr)
}

pub(crate) fn read_i32_with_type_check(bytes: &mut Bytes) -> Result<i32> {
    read_with_type_check(bytes, 3, |bytes| { Ok(bytes.get_i32_le()) })
}

pub(crate) fn read_i64_with_type_check(bytes: &mut Bytes) -> Result<i64> {
    read_with_type_check(bytes, 4, |bytes| { Ok(bytes.get_i64_le()) })
}

pub(crate) fn read_bool_with_type_check(bytes: &mut Bytes) -> Result<bool> {
    read_with_type_check(bytes, 8, |bytes| { Ok(read_bool(bytes)) })
}

pub(crate) fn read_string_optional_with_type_check(bytes: &mut Bytes) -> Result<Option<String>> {
    read_optional_with_type_check(bytes, 9, |bytes| { Ok(read_string(bytes)?) })
}

fn read_with_type_check<T, F>(bytes: &mut Bytes, expected_type_code: i8, value_reader: F) -> Result<T>
    where F: Fn(&mut Bytes) -> Result<T>
{
    let type_code = bytes.get_i8();

    if type_code == expected_type_code {
        Ok(value_reader(bytes)?)
    }
    else {
        Err(Error::new(ErrorKind::Serde, format!("Invalid type code: {}", type_code)))
    }
}

fn read_optional_with_type_check<T, F>(bytes: &mut Bytes, expected_type_code: i8, value_reader: F) -> Result<Option<T>>
    where F: Fn(&mut Bytes) -> Result<T>
{
    let type_code = bytes.get_i8();

    if type_code == 101 {
        Ok(None)
    }
    else {
        Ok(Some(read_with_type_check(bytes, expected_type_code, value_reader)?))
    }
}
