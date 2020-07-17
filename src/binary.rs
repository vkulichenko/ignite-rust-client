use std::any::type_name;

use bytes::{BufMut, Buf, BytesMut, Bytes};
use uuid::Uuid;
use num_traits::FromPrimitive;

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

impl IgniteRead for Value {
    fn read(bytes: &mut Bytes) -> Result<Value> {
        let type_code = *bytes.first().ok_or_else(|| Error::new(ErrorKind::Serde, "Out of bytes.".to_string()))?;

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
            _ => Err(Error::new(ErrorKind::Serde, format!("Invalid type code: {}", type_code))),
        }
    }
}

impl Nullable for Value {}

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

impl<T: BinaryWrite> BinaryWrite for Option<T> {
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

pub(crate) trait IgniteRead: Sized {
    fn read(bytes: &mut Bytes) -> Result<Self>;
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

impl Nullable for String {}

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

impl Nullable for Uuid {}

pub(crate) trait Nullable {}

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

pub(crate) trait EnumRead {}

impl<T: EnumRead + FromPrimitive> IgniteRead for T {
    fn read(bytes: &mut Bytes) -> Result<T> {
        let value: Option<T> = FromPrimitive::from_i32(IgniteRead::read(bytes)?);

        match value {
            Some(value) => Ok(value),
            None => Err(Error::new(ErrorKind::Serde, format!("Failed to read enum: {}", type_name::<T>()))),
        }
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
