use std::any::type_name;
use std::rc::Rc;
use std::cell::RefCell;

use bytes::{BytesMut, Bytes, Buf};
use num_traits::ToPrimitive;

use crate::binary::{Value, IgniteWrite, IgniteRead};
use crate::error::{Result, ErrorKind, Error};
use crate::network::Tcp;
use crate::configuration::CacheConfiguration;

#[derive(ToPrimitive, IgniteWrite)]
pub enum PeekMode {
    All = 0,
    Near = 1,
    Primary = 2,
    Backup = 3,
}

pub struct Cache {
    name: String,
    tcp: Rc<RefCell<Tcp>>,
}

impl Cache {
    pub(crate) fn new(name: String, tcp: Rc<RefCell<Tcp>>) -> Cache {
        Cache { name, tcp }
    }

    pub fn configuration(&self) -> Result<CacheConfiguration> {
        self.execute(
            1055,
            |_| { Ok(()) },
            |response| {
                response.advance(4); // Ignore length.

                CacheConfiguration::read(response)
            }
        )
    }

    pub fn get(&self, key: &Value) -> Result<Option<Value>> {
        self.execute(
            1000,
            |request| {
                key.write(request)
            },
            |response| {
                <Option<Value>>::read(response)
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
                bool::read(response)
            }
        )
    }

    pub fn get_all(&self, keys: &[Value]) -> Result<Vec<(Value, Option<Value>)>> {
        self.execute(
            1003,
            |request| {
                keys.write(request)
            },
            |response| {
                <Vec<(Value, Option<Value>)>>::read(response)
            }
        )
    }

    pub fn put_all(&self, entries: &[(Value, Value)]) -> Result<()> {
        self.execute(
            1004,
            |request| {
                entries.write(request)
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
                <Option<Value>>::read(response)
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
                <Option<Value>>::read(response)
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
                <Option<Value>>::read(response)
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
                <Option<Value>>::read(response)
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
                bool::read(response)
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
                bool::read(response)
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
                bool::read(response)
            }
        )
    }

    pub fn contains_keys(&self, keys: &[Value]) -> Result<bool> {
        self.execute(
            1012,
            |request| {
                keys.write(request)
            },
            |response| {
                bool::read(response)
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
                keys.write(request)
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
                bool::read(response)
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
                bool::read(response)
            }
        )
    }

    pub fn remove_keys(&self, keys: &[Value]) -> Result<()> {
        self.execute(
            1018,
            |request| {
                keys.write(request)
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
                peek_modes.write(request)
            },
            |response| {
                i64::read(response)
            }
        )
    }

    pub fn destroy(&self) -> Result<()> {
        self.tcp.borrow_mut().execute(
            1056,
            |request| {
                self.id().write(request)
            },
            |_| { Ok(()) }
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
                self.id().write(request)?;

                // Unused byte.
                request.advance(1);

                request_writer(request)
            },
            response_reader
        )
    }

    // TODO: Fails with overflow for some names
    fn id(&self) -> i32 {
        let mut hash = 0i64;

        for c in self.name.chars() {
            let c = c as i64;

            hash = 31 * hash + c;
        }

        hash as i32
    }
}
