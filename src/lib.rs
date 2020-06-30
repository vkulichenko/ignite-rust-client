use std::net::TcpStream;
use std::cell::{RefCell, RefMut};
use std::rc::Rc;
use bytes::{BytesMut, BufMut, Bytes, Buf};
use std::io::{Write, Read};
use crate::ErrorKind::{Network, Encoding, Ignite, Handshake};

#[cfg(test)]
mod tests {
    use crate::{Client, Configuration, Value};

    #[test]
    fn test_put_get_i8() {
        test_put_get(Value::I8(42), Value::I8(43), Value::I8(1));
    }

    #[test]
    fn test_put_get_i16() {
        test_put_get(Value::I16(42), Value::I16(43), Value::I16(1));
    }

    #[test]
    fn test_put_get_i32() {
        test_put_get(Value::I32(42), Value::I32(43), Value::I32(1));
    }

    #[test]
    fn test_put_get_i64() {
        test_put_get(Value::I64(42), Value::I64(43), Value::I64(1));
    }

    #[test]
    fn test_put_get_f64() {
        test_put_get(Value::F64(42.42), Value::F64(43.43), Value::F64(1.1));
    }

    #[test]
    fn test_put_get_f32() {
        test_put_get(Value::F32(42.42), Value::F32(43.43), Value::F32(1.1));
    }

    #[test]
    fn test_put_get_char() {
        test_put_get(Value::Char('a'), Value::Char('b'), Value::Char('1'));
    }

    #[test]
    fn test_put_get_bool() {
        test_put_get(Value::Bool(true), Value::Bool(false), Value::Bool(true));
    }

    #[test]
    fn test_put_get_string() {
        test_put_get(Value::String("42".to_string()), Value::String("43".to_string()), Value::String("1".to_string()));
    }

    fn test_put_get(existent_key: Value, non_existent_key: Value, value: Value) {
        let client = Client::start(Configuration::default())
            .expect("Failed to create a client.");

        let cache = client.cache("test-cache");

        assert_eq!(cache.clear(), Ok(()));
        assert_eq!(cache.get(&existent_key), Ok(None));
        assert_eq!(cache.put(&existent_key, &value), Ok(()));
        assert_eq!(cache.get(&existent_key), Ok(Some(value)));
        assert_eq!(cache.get(&non_existent_key), Ok(None));
    }
}

// === Version

#[derive(PartialEq, Debug)]
pub struct Version {
    major: i16,
    minor: i16,
    patch: i16,
}

// === Error

#[derive(PartialEq, Debug)]
pub enum ErrorKind {
    Network,
    Encoding,
    Handshake { server_version: Version, client_version: Version },
    Ignite(i32),
}

#[derive(PartialEq, Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

// === Configuration

pub struct Configuration {
    address: String,
}

impl Configuration {
    pub fn default() -> Configuration {
        Configuration {
            address: "127.0.0.1:10800".to_string(),
        }
    }

    pub fn address(mut self, address: &str) -> Configuration {
        self.address = address.to_string();

        self
    }
}

// === Client

pub struct Client {
    stream: Rc<RefCell<TcpStream>>,
}

impl Client {
    pub fn start(config: Configuration) -> Result<Client> {
        let stream = TcpStream::connect(&config.address)?;

        let stream = Rc::new(RefCell::new(stream));

        let mut request = BytesMut::with_capacity(8);

        request.put_i8(1);
        request.put_i16_le(VERSION.major);
        request.put_i16_le(VERSION.minor);
        request.put_i16_le(VERSION.patch);
        request.put_i8(2);

        let mut response = send(&mut stream.borrow_mut(), &request)?;

        let success = response.get_u8();

        if success == 1 {
            Ok(Client { stream })
        }
        else {
            let major = response.get_i16_le();
            let minor = response.get_i16_le();
            let patch = response.get_i16_le();

            let kind = Handshake {server_version: Version { major, minor, patch }, client_version: VERSION };

            let message = Value::read(&mut response)?;

            let message = match message {
                Some(Value::String(s)) => s,
                _ => "Handshake unexpected failure".to_string(),
            };

            Err(Error { kind, message })
        }
    }

    pub fn cache(&self, name: &str) -> Cache {
        Cache { name: name.to_string(), stream: self.stream.clone() }
    }
}

// === Cache

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

// === Value

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

// === Private

const VERSION: Version = Version { major: 1, minor: 0, patch: 0 };

type Result<T> = core::result::Result<T, Error>;

fn send(stream: &mut RefMut<TcpStream>, msg: &BytesMut) -> Result<Bytes> {
    // Write.

    let len = msg.len() as i32;
    let len = len.to_le_bytes();

    stream.write_all(&len)?;
    stream.write_all(msg.as_ref())?;
    stream.flush()?;

    // Read.

    let mut len = [0u8; 4];

    stream.read_exact(&mut len)?;

    let len = Bytes::from(len.to_vec()).get_i32_le();

    let mut msg = vec![0u8; len as usize];

    stream.read_exact(&mut msg)?;

    Ok(Bytes::from(msg))
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error { kind: Network, message: error.to_string() }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Error { kind: Encoding, message: error.to_string() }
    }
}

impl Cache {
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

        let mut response = send(&mut stream, &request)?;

        assert_eq!(response.get_i64_le(), 0); // Request ID.

        let status = response.get_i32_le();

        if status == 0 {
            response_reader(&mut response)
        }
        else {
            let message = String::from_utf8(response.to_vec())?;

            Err(Error {kind: Ignite(status), message })
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

impl Value {
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
                println!("char: {}", *v);
                println!("char u16: {}", *v as u16);

                if v.len_utf16() == 1 {
                    bytes.put_i8(7);
                    bytes.put_u16(*v as u16);
                }
                else {
                    result = Err(Error { kind: Encoding, message: "Only UTF-16 characters are supported.".to_string() });
                }
            }
            Value::Bool(v) => {
                bytes.put_i8(8);
                bytes.put_u8(if *v { 1 } else { 0 });
            }
            Value::String(v) => {
                let v = v.as_bytes();

                bytes.put_i8(9);
                bytes.put_i32_le(v.len() as i32);
                bytes.put_slice(v);
            }
        }

        result
    }

    fn read(bytes: &mut Bytes) -> Result<Option<Value>> {
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
                    Err(Error {kind: Encoding, message: format!("Failed to convert to char: {}", value) })
                }
            },
            8 => Ok(Some(Value::Bool(bytes.get_u8() != 0))),
            9 => {
                let len = bytes.get_i32_le() as usize;

                Ok(Some(Value::String(String::from_utf8(bytes.slice(.. len).to_vec())?)))
            },
            _ => Err(Error { kind: Ignite(0), message: format!("Invalid type code: {}", type_code) }),
        }
    }
}
