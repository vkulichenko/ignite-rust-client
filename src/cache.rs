use std::rc::Rc;
use std::cell::RefCell;

use bytes::{BytesMut, Bytes, BufMut, Buf};

use crate::binary::Value;
use crate::error::Result;
use crate::network::Tcp;

pub enum PeekMode {
    All,
    Near,
    Primary,
    Backup,
}

impl From<&PeekMode> for u8 {
    fn from(peek_mode: &PeekMode) -> Self {
        match peek_mode {
            PeekMode::All => 0,
            PeekMode::Near => 1,
            PeekMode::Primary => 2,
            PeekMode::Backup => 3,
        }
    }
}

pub struct Cache {
    name: String,
    tcp: Rc<RefCell<Tcp>>,
}

impl Cache {
    pub(crate) fn new(name: String, tcp: Rc<RefCell<Tcp>>) -> Cache {
        Cache { name, tcp }
    }

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

                let mut entries = Vec::with_capacity(len);

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

    pub fn contains_key(&self, key: &Value) -> Result<bool> {
        self.execute(
            1011,
            |request| {
                key.write(request)
            },
            |response| {
                Ok(response.get_i8() != 0)
            }
        )
    }

    pub fn contains_keys(&self, keys: &[Value]) -> Result<bool> {
        self.execute(
            1012,
            |request| {
                request.put_i32_le(keys.len() as i32);

                for key in keys {
                    key.write(request)?;
                }

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

    pub fn clear_key(&self, key: &Value) -> Result<()> {
        self.execute(
            1014,
            |request| {
                key.write(request)
            },
            |_| { Ok(()) }
        )
    }

    pub fn clear_keys(&self, keys: &[Value]) -> Result<()> {
        self.execute(
            1015,
            |request| {
                request.put_i32_le(keys.len() as i32);

                for key in keys {
                    key.write(request)?;
                }

                Ok(())
            },
            |_| { Ok(()) }
        )
    }

    pub fn remove_key(&self, key: &Value) -> Result<bool> {
        self.execute(
            1016,
            |request| {
                key.write(request)
            },
            |response| {
                Ok(response.get_i8() != 0)
            }
        )
    }

    pub fn remove_if_equals(&self, key: &Value, old_value: &Value) -> Result<bool> {
        self.execute(
            1017,
            |request| {
                key.write(request)?;
                old_value.write(request)?;

                Ok(())
            },
            |response| {
                Ok(response.get_i8() != 0)
            }
        )
    }

    pub fn remove_keys(&self, keys: &[Value]) -> Result<()> {
        self.execute(
            1018,
            |request| {
                request.put_i32_le(keys.len() as i32);

                for key in keys {
                    key.write(request)?;
                }

                Ok(())
            },
            |_| { Ok(()) }
        )
    }

    pub fn remove_all(&self) -> Result<()> {
        self.execute(
            1019,
            |_| { Ok(()) },
            |_| { Ok(()) }
        )
    }

    pub fn size(&self, peek_modes: &[PeekMode]) -> Result<i64> {
        self.execute(
            1020,
            |request| {
                request.put_i32_le(peek_modes.len() as i32);

                for peek_mode in peek_modes {
                    request.put_u8(u8::from(peek_mode));
                }

                Ok(())
            },
            |response| {
                Ok(response.get_i64_le())
            }
        )
    }

    fn execute<R, F1, F2>(&self, operation_code: i16, request_writer: F1, response_reader: F2) -> Result<R>
        where
            F1: Fn(&mut BytesMut) -> Result<()>,
            F2: Fn(&mut Bytes) -> Result<R>,
    {
        self.tcp.borrow_mut().execute(
            operation_code,
            |request| {
                request.put_i32_le(self.id());
                request.put_i8(0); // Unused.

                request_writer(request)
            },
            response_reader
        )
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
