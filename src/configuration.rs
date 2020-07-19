use std::any::type_name;

use bytes::{Bytes, BytesMut, BufMut};
use num_traits::{FromPrimitive, ToPrimitive};

use crate::error::{Result, ErrorKind, Error};
use crate::binary::{IgniteRead, Value, IgniteWrite};

pub struct Configuration {
    pub address: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Configuration {
    pub fn default() -> Configuration {
        Configuration {
            address: "127.0.0.1:10800".to_string(),
            username: None,
            password: None,
        }
    }

    pub fn address(mut self, address: &str) -> Configuration {
        self.address = address.to_string();

        self
    }

    pub fn username(mut self, username: &str) -> Configuration {
        self.username = Some(username.to_string());

        self
    }

    pub fn password(mut self, password: &str) -> Configuration {
        self.password = Some(password.to_string());

        self
    }
}

#[derive(FromPrimitive, ToPrimitive, IgniteRead, IgniteWrite)]
pub enum AtomicityMode {
    Transactional = 0,
    Atomic = 1,
    TransactionalSnapshot = 2,
}

#[derive(FromPrimitive, ToPrimitive, IgniteRead, IgniteWrite)]
pub enum CacheMode {
    Local = 0,
    Replicated = 1,
    Partitioned = 2,
}

#[derive(FromPrimitive, ToPrimitive, IgniteRead, IgniteWrite)]
pub enum PartitionLossPolicy {
    ReadOnlySafe = 0,
    ReadOnlyAll = 1,
    ReadWriteSafe = 2,
    ReadWriteAll = 3,
    Ignore = 4,
}

#[derive(FromPrimitive, ToPrimitive, IgniteRead, IgniteWrite)]
pub enum RebalanceMode {
    Sync = 0,
    Async = 1,
    None = 2,
}

#[derive(FromPrimitive, ToPrimitive, IgniteRead, IgniteWrite)]
pub enum WriteSynchronizationMode {
    FullSync = 0,
    FullAsync = 1,
    PrimarySync = 2,
}

#[derive(FromPrimitive, ToPrimitive, IgniteRead, IgniteWrite)]
pub enum IndexType {
    Sorted = 0,
    FullText = 1,
    Geospatial = 2,
}

#[derive(IgniteRead, IgniteWrite)]
pub struct CacheKeyConfiguration {
    pub type_name: String,
    pub affinity_key_field_name: String,
}

#[derive(IgniteRead, IgniteWrite)]
pub struct QueryField {
    pub name: String,
    pub type_name: String,
    pub key_field: bool,
    pub not_null: bool,
    pub default_value: Option<Value>,
}

#[derive(IgniteRead, IgniteWrite)]
pub struct QueryIndex {
    pub index_name: String,
    pub index_type: IndexType,
    pub inline_size: i32,
    pub fields: Vec<(String, bool)>,
}

#[derive(IgniteRead, IgniteWrite)]
pub struct QueryEntity {
    pub key_type_name: String,
    pub value_type_name: String,
    pub table_name: String,
    pub key_field_name: String,
    pub value_field_name: String,
    pub fields: Vec<QueryField>,
    pub aliases: Vec<(String, String)>,
    pub indexes: Vec<QueryIndex>,
}

#[derive(IgniteRead)]
pub struct CacheConfiguration {
    pub atomicity_mode: AtomicityMode,
    pub backups: i32,
    pub mode: CacheMode,
    pub copy_on_read: bool,
    pub data_region_name: Option<String>,
    pub eager_ttl: bool,
    pub statistics_enabled: bool,
    pub group_name: Option<String>,
    pub default_lock_timeout: i64,
    pub max_concurrent_async_operations: i32,
    pub max_query_iterators: i32,
    pub name: String,
    pub on_heap_cache_enabled: bool,
    pub partition_loss_policy: PartitionLossPolicy,
    pub query_detail_metrics_size: i32,
    pub query_parallelism: i32,
    pub read_from_backup: bool,
    pub rebalance_batch_size: i32,
    pub rebalance_batch_prefetch_count: i64,
    pub rebalance_delay: i64,
    pub rebalance_mode: RebalanceMode,
    pub rebalance_order: i32,
    pub rebalance_throttle: i64,
    pub rebalance_timeout: i64,
    pub sql_escape_all: bool,
    pub sql_index_inline_max_size: i32,
    pub sql_schema: Option<String>,
    pub write_synchronization_mode: WriteSynchronizationMode,
    pub cache_key_configurations: Vec<CacheKeyConfiguration>,
    pub query_entities: Vec<QueryEntity>,
}

impl CacheConfiguration {
    pub fn default(name: &str) -> CacheConfiguration {
        CacheConfiguration {
            atomicity_mode: AtomicityMode::Atomic,
            backups: 0,
            mode: CacheMode::Partitioned,
            copy_on_read: true,
            data_region_name: None,
            eager_ttl: true,
            statistics_enabled: false,
            group_name: None,
            default_lock_timeout: 0,
            max_concurrent_async_operations: 500,
            max_query_iterators: 1024,
            name: name.to_string(),
            on_heap_cache_enabled: false,
            partition_loss_policy: PartitionLossPolicy::Ignore,
            query_detail_metrics_size: 0,
            query_parallelism: 1,
            read_from_backup: true,
            rebalance_batch_size: 512 * 1024,
            rebalance_batch_prefetch_count: 3,
            rebalance_delay: 0,
            rebalance_mode: RebalanceMode::Async,
            rebalance_order: 0,
            rebalance_throttle: 0,
            rebalance_timeout: 10000,
            sql_escape_all: false,
            sql_index_inline_max_size: -1,
            sql_schema: None,
            write_synchronization_mode: WriteSynchronizationMode::PrimarySync,
            cache_key_configurations: Vec::new(),
            query_entities: Vec::new(),
        }
    }
}

macro_rules! write_property {
    ($bytes:expr, $count:expr, $code:expr, $prop:expr) => {
        $bytes.put_i16_le($code);
        $prop.write($bytes)?;
        $count = $count + 1;
    };
}

impl IgniteWrite for CacheConfiguration {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        let mut config_bytes = BytesMut::with_capacity(1024);
        let mut count = 0i16;

        write_property!(&mut config_bytes, count, 2, self.atomicity_mode);
        write_property!(&mut config_bytes, count, 3, self.backups);
        write_property!(&mut config_bytes, count, 1, self.mode);
        write_property!(&mut config_bytes, count, 5, self.copy_on_read);
        write_property!(&mut config_bytes, count, 100, self.data_region_name);
        write_property!(&mut config_bytes, count, 405, self.eager_ttl);
        write_property!(&mut config_bytes, count, 406, self.statistics_enabled);
        write_property!(&mut config_bytes, count, 400, self.group_name);
        write_property!(&mut config_bytes, count, 402, self.default_lock_timeout);
        write_property!(&mut config_bytes, count, 403, self.max_concurrent_async_operations);
        write_property!(&mut config_bytes, count, 206, self.max_query_iterators);
        write_property!(&mut config_bytes, count, 0, self.name);
        write_property!(&mut config_bytes, count, 101, self.on_heap_cache_enabled);
        write_property!(&mut config_bytes, count, 404, self.partition_loss_policy);
        write_property!(&mut config_bytes, count, 202, self.query_detail_metrics_size);
        write_property!(&mut config_bytes, count, 201, self.query_parallelism);
        write_property!(&mut config_bytes, count, 6, self.read_from_backup);
        write_property!(&mut config_bytes, count, 303, self.rebalance_batch_size);
        write_property!(&mut config_bytes, count, 304, self.rebalance_batch_prefetch_count);
        write_property!(&mut config_bytes, count, 301, self.rebalance_delay);
        write_property!(&mut config_bytes, count, 300, self.rebalance_mode);
        write_property!(&mut config_bytes, count, 305, self.rebalance_order);
        write_property!(&mut config_bytes, count, 306, self.rebalance_throttle);
        write_property!(&mut config_bytes, count, 302, self.rebalance_timeout);
        write_property!(&mut config_bytes, count, 205, self.sql_escape_all);
        write_property!(&mut config_bytes, count, 204, self.sql_index_inline_max_size);
        write_property!(&mut config_bytes, count, 203, self.sql_schema);
        write_property!(&mut config_bytes, count, 4, self.write_synchronization_mode);
        write_property!(&mut config_bytes, count, 401, self.cache_key_configurations);
        write_property!(&mut config_bytes, count, 200, self.query_entities);

        bytes.put_i32_le(2 + config_bytes.len() as i32);
        bytes.put_i16_le(count);
        bytes.put(config_bytes);

        Ok(())
    }
}
