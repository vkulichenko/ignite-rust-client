use std::rc::Rc;
use std::cell::RefCell;
use std::net::TcpStream;

use bytes::{BytesMut, Bytes, BufMut, Buf};

use crate::binary::Value;
use crate::error::{Result, ErrorKind, Error};
use crate::network;

pub struct Cache {
    name: String,
    stream: Rc<RefCell<TcpStream>>,
}

impl Cache {
    pub fn get(&self, key: &Value) -> Result<Option<Value>> {
        self.execute(
            1000,
            |request| {
                key.write(request)
            },
            |response| {
                Value::read(response)
            }
        )
    }

    pub fn put(&self, key: &Value, value: &Value) -> Result<()> {
        self.execute(
            1001,
            |request| {
                key.write(request)?;
                value.write(request)?;

                Ok(())
            },
            |_| { Ok(()) }
        )
    }

    pub fn put_if_absent(&self, key: &Value, value: &Value) -> Result<bool> {
        self.execute(
            1002,
            |request| {
                key.write(request)?;
                value.write(request)?;

                Ok(())
            },
            |response| {
                Ok(response.get_i8() != 0)
            }
        )
    }

    pub fn get_all(&self, keys: &[Value]) -> Result<Vec<(Value, Option<Value>)>> {
        self.execute(
            1003,
            |request| {
                request.put_i32_le(keys.len() as i32);

                for key in keys {
                    key.write(request)?;
                }

                Ok(())
            },
            |response| {
                let len = response.get_i32_le() as usize;

                let mut entries: Vec<(Value, Option<Value>)> = Vec::with_capacity(len);

                for _ in 0 .. len {
                    let key = Value::read(response)?;
                    let value = Value::read(response)?;

                    if let Some(key) = key {
                        entries.push((key, value));
                    }
                }

                Ok(entries)
            }
        )
    }

    pub fn put_all(&self, entries: &[(Value, Value)]) -> Result<()> {
        self.execute(
            1004,
            |request| {
                request.put_i32_le(entries.len() as i32);

                for (key, value) in entries {
                    key.write(request)?;
                    value.write(request)?;
                }

                Ok(())
            },
            |_| { Ok(()) }
        )
    }

    pub fn get_and_put(&self, key: &Value, value: &Value) -> Result<Option<Value>> {
        self.execute(
            1005,
            |request| {
                key.write(request)?;
                value.write(request)?;

                Ok(())
            },
            |response| {
                Value::read(response)
            }
        )
    }

    pub fn get_and_replace(&self, key: &Value, value: &Value) -> Result<Option<Value>> {
        self.execute(
            1006,
            |request| {
                key.write(request)?;
                value.write(request)?;

                Ok(())
            },
            |response| {
                Value::read(response)
            }
        )
    }

    pub fn get_and_remove(&self, key: &Value) -> Result<Option<Value>> {
        self.execute(
            1007,
            |request| {
                key.write(request)
            },
            |response| {
                Value::read(response)
            }
        )
    }

    pub fn get_and_put_if_absent(&self, key: &Value, value: &Value) -> Result<Option<Value>> {
        self.execute(
            1008,
            |request| {
                key.write(request)?;
                value.write(request)?;

                Ok(())
            },
            |response| {
                Value::read(response)
            }
        )
    }

    pub fn replace(&self, key: &Value, value: &Value) -> Result<bool> {
        self.execute(
            1009,
            |request| {
                key.write(request)?;
                value.write(request)?;

                Ok(())
            },
            |response| {
                Ok(response.get_i8() != 0)
            }
        )
    }

    pub fn replace_if_equals(&self, key: &Value, old_value: &Value, new_value: &Value) -> Result<bool> {
        self.execute(
            1010,
            |request| {
                key.write(request)?;
                old_value.write(request)?;
                new_value.write(request)?;

                Ok(())
            },
            |response| {
                Ok(response.get_i8() != 0)
            }
        )
    }

    pub fn clear(&self) -> Result<()> {
        self.execute(
            1013,
            |_| { Ok(()) },
            |_| { Ok(()) }
        )
    }
}

impl Cache {
    pub(crate) fn new(name: String, stream: Rc<RefCell<TcpStream>>) -> Cache {
        Cache { name, stream }
    }

    fn execute<R, F1, F2>(&self, operation_code: i16, request_writer: F1, response_reader: F2) -> Result<R>
        where
            F1: Fn(&mut BytesMut) -> Result<()>,
            F2: Fn(&mut Bytes) -> Result<R>,
    {
        let mut stream = self.stream.borrow_mut();

        let mut request = BytesMut::with_capacity(1024);

        request.put_i16_le(operation_code);
        request.put_i64_le(0); // Request ID.
        request.put_i32_le(self.id());
        request.put_i8(0); // Unused.

        request_writer(&mut request)?;

        let mut response = network::send(&mut stream, &request)?;

        assert_eq!(response.get_i64_le(), 0); // Request ID.

        let status = response.get_i32_le();

        if status == 0 {
            response_reader(&mut response)
        }
        else {
            let message = String::from_utf8(response.to_vec())?;

            Err(Error::new(ErrorKind::Ignite(status), message))
        }
    }

    fn id(&self) -> i32 {
        let mut hash = 0i64;

        for c in self.name.chars() {
            let c = c as i64;

            hash = 31 * hash + c;
        }

        hash as i32
    }
}
