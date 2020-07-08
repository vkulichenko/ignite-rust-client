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
            i8::TYPE_CODE => Ok(Some(Value::I8(i8::read(bytes)?))),
            i16::TYPE_CODE => Ok(Some(Value::I16(i16::read(bytes)?))),
            i32::TYPE_CODE => Ok(Some(Value::I32(i32::read(bytes)?))),
            i64::TYPE_CODE => Ok(Some(Value::I64(i64::read(bytes)?))),
            f32::TYPE_CODE => Ok(Some(Value::F32(f32::read(bytes)?))),
            f64::TYPE_CODE => Ok(Some(Value::F64(f64::read(bytes)?))),
            char::TYPE_CODE => Ok(Some(Value::Char(char::read(bytes)?))),
            bool::TYPE_CODE => Ok(Some(Value::Bool(bool::read(bytes)?))),
            String::TYPE_CODE => Ok(Some(Value::String(String::read(bytes)?))),
            Uuid::TYPE_CODE => Ok(Some(Value::Uuid(Uuid::read(bytes)?))),
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

pub(crate) trait BinaryRead: Sized {
    const TYPE_CODE: i8;

    fn read(bytes: &mut Bytes) -> Result<Self>;
}

impl BinaryRead for i8 {
    const TYPE_CODE: i8 = 1;

    fn read(bytes: &mut Bytes) -> Result<i8> {
        Ok(bytes.get_i8())
    }
}

impl BinaryRead for i16 {
    const TYPE_CODE: i8 = 2;

    fn read(bytes: &mut Bytes) -> Result<i16> {
        Ok(bytes.get_i16_le())
    }
}

impl BinaryRead for i32 {
    const TYPE_CODE: i8 = 3;

    fn read(bytes: &mut Bytes) -> Result<i32> {
        Ok(bytes.get_i32_le())
    }
}

impl BinaryRead for i64 {
    const TYPE_CODE: i8 = 4;

    fn read(bytes: &mut Bytes) -> Result<i64> {
        Ok(bytes.get_i64_le())
    }
}

impl BinaryRead for f32 {
    const TYPE_CODE: i8 = 5;

    fn read(bytes: &mut Bytes) -> Result<f32> {
        Ok(bytes.get_f32_le())
    }
}

impl BinaryRead for f64 {
    const TYPE_CODE: i8 = 6;

    fn read(bytes: &mut Bytes) -> Result<f64> {
        Ok(bytes.get_f64_le())
    }
}

impl BinaryRead for char {
    const TYPE_CODE: i8 = 7;

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

impl BinaryRead for bool {
    const TYPE_CODE: i8 = 8;

    fn read(bytes: &mut Bytes) -> Result<bool> {
        Ok(bytes.get_u8() != 0)
    }
}

impl BinaryRead for String {
    const TYPE_CODE: i8 = 9;

    fn read(bytes: &mut Bytes) -> Result<String> {
        let len = bytes.get_i32_le() as usize;

        let vec = bytes.slice(..len).to_vec();

        bytes.advance(len);

        Ok(String::from_utf8(vec)?)
    }
}

impl BinaryRead for Uuid {
    const TYPE_CODE: i8 = 10;

    fn read(bytes: &mut Bytes) -> Result<Uuid> {
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

pub(crate) fn read<T>(bytes: &mut Bytes) -> Result<T>
    where T: BinaryRead
{
    let type_code = bytes.get_i8();

    match type_code {
        type_code if type_code == T::TYPE_CODE => Ok(T::read(bytes)?),
        101 => Err(Error::new(ErrorKind::Serde, "NULL is not expected.".to_string())),
        _ => Err(Error::new(ErrorKind::Serde, format!("Unexpected type code: {}", type_code))),
    }
}

pub(crate) fn read_optional<T>(bytes: &mut Bytes) -> Result<Option<T>>
    where T: BinaryRead
{
    let type_code = bytes.get_i8();

    match type_code {
        type_code if type_code == T::TYPE_CODE => Ok(Some(T::read(bytes)?)),
        101 => Ok(None),
        _ => Err(Error::new(ErrorKind::Serde, format!("Unexpected type code: {}", type_code))),
    }
}

pub(crate) fn read_multiple<T>(bytes: &mut Bytes) -> Result<Vec<T>>
    where T: BinaryRead
{
    let len = bytes.get_i32_le() as usize;

    let mut vec = Vec::with_capacity(len);

    for _ in 0 .. len {
        vec.push(read(bytes)?);
    }

    Ok(vec)
}
