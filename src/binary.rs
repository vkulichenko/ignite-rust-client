use bytes::{BufMut, Buf, BytesMut, Bytes};

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
            7 => {
                let value = bytes.get_u16_le();

                if let Some(char) = std::char::from_u32(value as u32) {
                    Ok(Some(Value::Char(char)))
                }
                else {
                    Err(Error::new(ErrorKind::Encoding, format!("Failed to convert to char: {}", value)))
                }
            },
            8 => Ok(Some(Value::Bool(bytes.get_u8() != 0))),
            9 => {
                let len = bytes.get_i32_le() as usize;

                let vec = bytes.slice(..len).to_vec();

                bytes.advance(len);

                Ok(Some(Value::String(String::from_utf8(vec)?)))
            },
            _ => Err(Error::new(ErrorKind::Ignite(0), format!("Invalid type code: {}", type_code))),
        }
    }
}

pub(crate) trait BinaryWrite {
    fn write(&self, bytes: &mut BytesMut) -> Result<()>;
}

impl BinaryWrite for Value {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut result = Ok(());

        match self {
            Value::I8(v) => {
                bytes.put_i8(1);
                bytes.put_i8(*v);
            }
            Value::I16(v) => {
                bytes.put_i8(2);
                bytes.put_i16_le(*v);
            }
            Value::I32(v) => {
                bytes.put_i8(3);
                bytes.put_i32_le(*v);
            },
            Value::I64(v) => {
                bytes.put_i8(4);
                bytes.put_i64_le(*v);
            },
            Value::F32(v) => {
                bytes.put_i8(5);
                bytes.put_f32_le(*v);
            }
            Value::F64(v) => {
                bytes.put_i8(6);
                bytes.put_f64_le(*v);
            }
            Value::Char(v) => {
                if v.len_utf16() == 1 {
                    bytes.put_i8(7);
                    bytes.put_u16(*v as u16);
                }
                else {
                    result = Err(Error::new(ErrorKind::Encoding, "Only UTF-16 characters are supported.".to_string()));
                }
            }
            Value::Bool(v) => {
                bytes.put_i8(8);
                bytes.put_u8(if *v { 1 } else { 0 });
            }
            Value::String(v) => {
                result = v.write(bytes);
            }
        }

        result
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
