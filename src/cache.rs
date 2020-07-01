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
            |mut request| {
                key.write(&mut request)
            },
            |response| {
                Value::read(response)
            }
        )
    }

    pub fn put(&self, key: &Value, value: &Value) -> Result<()> {
        self.execute(
            1001,
            |mut request| {
                key.write(&mut request)?;
                value.write(&mut request)?;

                Ok(())
            },
            |_| { Ok(()) }
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
