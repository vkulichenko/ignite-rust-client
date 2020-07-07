#[macro_use]
extern crate num_derive;

mod configuration;
mod binary;
mod cache;
mod error;
mod network;

use std::net::TcpStream;
use std::rc::Rc;
use std::cell::RefCell;

use bytes::Buf;

use configuration::Configuration;
use cache::Cache;
use error::Result;
use network::Tcp;
use binary::{Value, BinaryWrite};

#[derive(PartialEq, Debug)]
pub struct Version {
    major: i16,
    minor: i16,
    patch: i16,
}

pub const VERSION: Version = Version { major: 1, minor: 1, patch: 0 };

pub struct Client {
    tcp: Rc<RefCell<Tcp>>,
}

impl Client {
    pub fn start(config: Configuration) -> Result<Client> {
        let stream = TcpStream::connect(&config.address)?;

        let tcp = Rc::new(RefCell::new(Tcp { stream }));

        tcp.borrow_mut().handshake(&config)?;

        Ok(Client { tcp })
    }

    pub fn cache_names(&self) -> Result<Vec<String>> {
        self.tcp.borrow_mut().execute(
            1050,
            |_| { Ok(()) },
            |response| {
                let len = response.get_i32_le() as usize;

                let mut names = Vec::with_capacity(len);

                for _ in 0 .. len {
                    let name = Value::read(response)?;

                    if let Some(Value::String(name)) = name {
                        names.push(name);
                    }
                }

                Ok(names)
            }
        )
    }

    pub fn create_cache(&self, name: &str) -> Result<Cache> {
        let name = name.to_string();

        self.tcp.borrow_mut().execute(
            1051,
            |request| {
                name.clone().write(request)
            },
            |_| { Ok(()) }
        )?;

        Ok(Cache::new(name.clone(), self.tcp.clone()))
    }

    pub fn get_or_create_cache(&self, name: &str) -> Result<Cache> {
        let name = name.to_string();

        self.tcp.borrow_mut().execute(
            1052,
            |request| {
                name.clone().write(request)
            },
            |_| { Ok(()) }
        )?;

        Ok(Cache::new(name.clone(), self.tcp.clone()))
    }

    pub fn destroy_cache(&self, name: &str) -> Result<()> {
        self.cache(name).destroy()
    }

    pub fn cache(&self, name: &str) -> Cache {
        Cache::new(name.to_string(), self.tcp.clone())
    }
}

// === Tests

#[cfg(test)]
mod tests {
    use crate::{Configuration, Client};
    use crate::binary::Value;
    use crate::cache::{Cache, PeekMode};
    use uuid::Uuid;

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

    #[test]
    fn test_put_get_uuid() {
        test_put_get(Value::Uuid(Uuid::from_u128(1234)), Value::Uuid(Uuid::from_u128(4321)), Value::Uuid(Uuid::from_u128(1234)));
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

    #[test]
    fn test_clear_key() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.clear_key(&Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.clear_key(&Value::I32(2)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.put(&Value::I32(1), &Value::I32(1)), Ok(()));
        assert_eq!(cache.put(&Value::I32(2), &Value::I32(2)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.clear_key(&Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.clear_key(&Value::I32(2)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
    }

    #[test]
    fn test_clear_keys() {
        let cache = cache();

        let keys = vec![Value::I32(1), Value::I32(2)];

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.get(&Value::I32(3)), Ok(None));
        assert_eq!(cache.clear_keys(keys.as_slice()), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.get(&Value::I32(3)), Ok(None));
        assert_eq!(cache.put(&Value::I32(1), &Value::I32(1)), Ok(()));
        assert_eq!(cache.put(&Value::I32(2), &Value::I32(2)), Ok(()));
        assert_eq!(cache.put(&Value::I32(3), &Value::I32(3)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.get(&Value::I32(3)), Ok(Some(Value::I32(3))));
        assert_eq!(cache.clear_keys(keys.as_slice()), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.get(&Value::I32(3)), Ok(Some(Value::I32(3))));
    }

    #[test]
    fn test_remove_key() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.remove_key(&Value::I32(1)), Ok(false));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.remove_key(&Value::I32(2)), Ok(false));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.put(&Value::I32(1), &Value::I32(1)), Ok(()));
        assert_eq!(cache.put(&Value::I32(2), &Value::I32(2)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.remove_key(&Value::I32(1)), Ok(true));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.remove_key(&Value::I32(2)), Ok(true));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
    }

    #[test]
    fn test_remove_if_equals() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.remove_if_equals(&Value::I32(42), &Value::I32(1)), Ok(false));
        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
        assert_eq!(cache.put(&Value::I32(42), &Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.remove_if_equals(&Value::I32(42), &Value::I32(0)), Ok(false));
        assert_eq!(cache.get(&Value::I32(42)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.remove_if_equals(&Value::I32(42), &Value::I32(1)), Ok(true));
        assert_eq!(cache.get(&Value::I32(42)), Ok(None));
    }

    #[test]
    fn test_remove_keys() {
        let cache = cache();

        let keys = vec![Value::I32(1), Value::I32(2)];

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.get(&Value::I32(3)), Ok(None));
        assert_eq!(cache.remove_keys(keys.as_slice()), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.get(&Value::I32(3)), Ok(None));
        assert_eq!(cache.put(&Value::I32(1), &Value::I32(1)), Ok(()));
        assert_eq!(cache.put(&Value::I32(2), &Value::I32(2)), Ok(()));
        assert_eq!(cache.put(&Value::I32(3), &Value::I32(3)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.get(&Value::I32(3)), Ok(Some(Value::I32(3))));
        assert_eq!(cache.remove_keys(keys.as_slice()), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.get(&Value::I32(3)), Ok(Some(Value::I32(3))));
    }

    #[test]
    fn test_remove_all() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.remove_all(), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.put(&Value::I32(1), &Value::I32(1)), Ok(()));
        assert_eq!(cache.put(&Value::I32(2), &Value::I32(2)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.remove_all(), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
    }

    #[test]
    fn test_size() {
        let cache = cache();

        assert_eq!(cache.get(&Value::I32(1)), Ok(None));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.size(&[]), Ok(0));
        assert_eq!(cache.size(&[PeekMode::Primary]), Ok(0));
        assert_eq!(cache.put(&Value::I32(1), &Value::I32(1)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(None));
        assert_eq!(cache.size(&[]), Ok(1));
        assert_eq!(cache.size(&[PeekMode::Primary]), Ok(1));
        assert_eq!(cache.put(&Value::I32(2), &Value::I32(2)), Ok(()));
        assert_eq!(cache.get(&Value::I32(1)), Ok(Some(Value::I32(1))));
        assert_eq!(cache.get(&Value::I32(2)), Ok(Some(Value::I32(2))));
        assert_eq!(cache.size(&[]), Ok(2));
        assert_eq!(cache.size(&[PeekMode::Primary]), Ok(2));
    }

    #[test]
    fn test_cache_names() {
        let client = client();

        let mut expected_names = vec!["test-cache", "another-cache"];

        expected_names.sort();

        let mut names = client.cache_names()
            .expect("Failed to execute cache_names() operation.");

        names.sort();

        assert_eq!(names, expected_names);
    }

    #[test]
    fn test_create_cache() {
        let client = client();

        assert!(!client.cache_names()
            .expect("Failed to get cache names.")
            .contains(&"new-cache".to_string()));

        let cache = client.create_cache("new-cache")
            .expect("Failed to create cache.");

        assert!(client.cache_names()
            .expect("Failed to get cache names.")
            .contains(&"new-cache".to_string()));

        assert!(client.create_cache("new-cache").is_err());

        cache.destroy()
            .expect("Failed to destroy cache.");

        assert!(!client.cache_names()
            .expect("Failed to get cache names.")
            .contains(&"new-cache".to_string()));
    }

    #[test]
    fn test_get_or_create_cache() {
        let client = client();

        assert!(!client.cache_names()
            .expect("Failed to get cache names.")
            .contains(&"new-cache".to_string()));

        let cache = client.get_or_create_cache("new-cache")
            .expect("Failed to create cache.");

        assert!(client.cache_names()
            .expect("Failed to get cache names.")
            .contains(&"new-cache".to_string()));

        cache.destroy()
            .expect("Failed to destroy cache.");

        assert!(!client.cache_names()
            .expect("Failed to get cache names.")
            .contains(&"new-cache".to_string()));
    }

    #[test]
    fn test_get_configuration() {
        let cache = cache();

        let config = cache.configuration()
            .expect("Failed to get cache configuration.");

        assert_eq!(config.name, Some("test-cache".to_string()));

        // TODO: Check other parameters.
    }

    fn client() -> Client {
        let config = Configuration::default()
            .username("ignite")
            .password("ignite");

        Client::start(config)
            .expect("Failed to create a client.")
    }

    fn cache() -> Cache {
        let client = client();

        let cache = client.cache("test-cache");

        assert_eq!(cache.clear(), Ok(()));

        cache
    }
}
