mod configuration;
mod binary;
mod cache;
mod error;
mod network;

use std::net::TcpStream;
use std::rc::Rc;
use std::cell::RefCell;

use bytes::{BytesMut, BufMut, Buf};

use configuration::Configuration;
use binary::Value;
use cache::Cache;
use error::{Result, ErrorKind, Error};

#[derive(PartialEq, Debug)]
pub struct Version {
    major: i16,
    minor: i16,
    patch: i16,
}

pub const VERSION: Version = Version { major: 1, minor: 1, patch: 0 };

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

        if let Some(username) = config.username {
            Value::String(username).write(&mut request)?;

            match config.password {
                Some(password) => {
                    Value::String(password).write(&mut request)?;
                }
                None => {
                    Value::write_null(&mut request)?;
                }
            }
        }

        let mut response = network::send(&mut stream.borrow_mut(), &request)?;

        let success = response.get_u8();

        if success == 1 {
            Ok(Client { stream })
        }
        else {
            let major = response.get_i16_le();
            let minor = response.get_i16_le();
            let patch = response.get_i16_le();

            let kind = ErrorKind::Handshake {server_version: Version { major, minor, patch }, client_version: VERSION };

            let message = Value::read(&mut response)?;

            let message = match message {
                Some(Value::String(message)) => message,
                _ => "Handshake unexpected failure".to_string(),
            };

            Err(Error::new(kind, message))
        }
    }

    pub fn cache(&self, name: &str) -> Cache {
        Cache::new(name.to_string(), self.stream.clone())
    }
}

// === Tests

#[cfg(test)]
mod tests {
    use crate::{Configuration, Client};
    use crate::binary::Value;
    use crate::cache::Cache;

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

    // #[test] TODO: fix
    // fn test_put_get_char() {
    //     test_put_get(Value::Char('a'), Value::Char('b'), Value::Char('1'));
    // }

    #[test]
    fn test_put_get_bool() {
        test_put_get(Value::Bool(true), Value::Bool(false), Value::Bool(true));
    }

    #[test]
    fn test_put_get_string() {
        test_put_get(Value::String("42".to_string()), Value::String("43".to_string()), Value::String("1".to_string()));
    }

    fn test_put_get(existent_key: Value, non_existent_key: Value, value: Value) {
        let cache = cache();

        assert_eq!(cache.get(&existent_key), Ok(None));
        assert_eq!(cache.put(&existent_key, &value), Ok(()));
        assert_eq!(cache.get(&existent_key), Ok(Some(value)));
        assert_eq!(cache.get(&non_existent_key), Ok(None));
    }

    #[test]
    fn test_put_if_absent() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.put_if_absent(&Value::I32(42), &Value::I32(1)), Ok(true));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.put_if_absent(&Value::I32(42), &Value::I32(2)), Ok(false));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
    }

    #[test]
    fn test_get_all() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.get(&Value::I32(3)), Ok(None));

        assert_eq!(cache.put(&Value::I32(1), &Value::I32(1)), Ok(()));
        assert_eq!(cache.put(&Value::I32(2), &Value::I32(2)), Ok(()));
        assert_eq!(cache.put(&Value::I32(3), &Value::I32(3)), Ok(()));

        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.get(&Value::I32(3)), Ok(Some(Value::I32(3))));

        let keys = vec![
            Value::I32(1),
            Value::I32(2),
            Value::I32(3),
        ];

        let entries = vec![
            (Value::I32(1), Some(Value::I32(1))),
            (Value::I32(2), Some(Value::I32(2))),
            (Value::I32(3), Some(Value::I32(3))),
        ];

        assert_eq!(cache.get_all(keys.as_slice()), Ok(entries));
    }

    #[test]
    fn test_put_all() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.get(&Value::I32(3)), Ok(None));

        let entries = vec![
            (Value::I32(1), Value::I32(1)),
            (Value::I32(2), Value::I32(2)),
            (Value::I32(3), Value::I32(3)),
        ];

        assert_eq!(cache.put_all(entries.as_slice()), Ok(()));

        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.get(&Value::I32(3)), Ok(Some(Value::I32(3))));
    }

    #[test]
    fn test_get_and_put() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.get_and_put(&Value::I32(42), &Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get_and_put(&Value::I32(42), &Value::I32(2)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(2))));
    }

    #[test]
    fn test_get_and_replace() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.get_and_replace(&Value::I32(42), &Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.put(&Value::I32(42), &Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get_and_replace(&Value::I32(42), &Value::I32(2)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(2))));
    }

    #[test]
    fn test_get_and_remove() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.get_and_remove(&Value::I32(42)), Ok(None));
        assert_eq!(cache.put(&Value::I32(42), &Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get_and_remove(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
    }

    #[test]
    fn test_get_and_put_if_absent() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.get_and_put_if_absent(&Value::I32(42), &Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get_and_put_if_absent(&Value::I32(42), &Value::I32(2)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
    }

    #[test]
    fn test_replace() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.replace(&Value::I32(42), &Value::I32(1)), Ok(false));
        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.put(&Value::I32(42), &Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.replace(&Value::I32(42), &Value::I32(2)), Ok(true));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(2))));
    }

    #[test]
    fn test_replace_if_equals() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.replace_if_equals(&Value::I32(42), &Value::I32(1), &Value::I32(2)), Ok(false));
        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.put(&Value::I32(42), &Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.replace_if_equals(&Value::I32(42), &Value::I32(1), &Value::I32(2)), Ok(true));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.replace_if_equals(&Value::I32(42), &Value::I32(0), &Value::I32(3)), Ok(false));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(2))));
    }

    #[test]
    fn test_contains_key() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.contains_key(&Value::I32(42)), Ok(false));
        assert_eq!(cache.put(&Value::I32(42), &Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.contains_key(&Value::I32(42)), Ok(true));
    }

    #[test]
    fn test_contains_keys() {
        let cache = cache();

        let keys = vec![Value::I32(1), Value::I32(2)];

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.contains_keys(keys.as_slice()), Ok(false));
        assert_eq!(cache.put(&Value::I32(1), &Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.contains_keys(keys.as_slice()), Ok(false));
        assert_eq!(cache.put(&Value::I32(2), &Value::I32(2)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.contains_keys(keys.as_slice()), Ok(true));
    }

    fn cache() -> Cache {
        let config = Configuration::default()
            .username("ignite")
            .password("ignite");

        let client = Client::start(config)
            .expect("Failed to create a client.");

        let cache = client.cache("test-cache");

        assert_eq!(cache.clear(), Ok(()));

        cache
    }
}
