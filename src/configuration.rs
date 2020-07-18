use std::any::type_name;

use bytes::{Bytes, BytesMut, BufMut};
use num_traits::{FromPrimitive, ToPrimitive};

use crate::error::{Result, ErrorKind, Error};
use crate::binary::{IgniteRead, Value, IgniteWrite};

pub struct Configuration {
    pub(crate) address: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
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

impl IgniteWrite for CacheConfiguration {
    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
        let config_bytes = BytesMut::with_capacity(1024);

        write_property(bytes, 2, &self.atomicity_mode)?;
        write_property(bytes, 3, &self.backups)?;
        write_property(bytes, 1, &self.mode)?;
        write_property(bytes, 5, &self.copy_on_read)?;
        write_property(bytes, 100, &self.data_region_name)?;
        write_property(bytes, 405, &self.eager_ttl)?;
        write_property(bytes, 406, &self.statistics_enabled)?;
        write_property(bytes, 400, &self.group_name)?;
        write_property(bytes, 402, &self.default_lock_timeout)?;
        write_property(bytes, 403, &self.max_concurrent_async_operations)?;
        write_property(bytes, 206, &self.max_query_iterators)?;
        write_property(bytes, 0, &self.name)?;
        write_property(bytes, 101, &self.on_heap_cache_enabled)?;
        write_property(bytes, 404, &self.partition_loss_policy)?;
        write_property(bytes, 202, &self.query_detail_metrics_size)?;
        write_property(bytes, 201, &self.query_parallelism)?;
        write_property(bytes, 6, &self.read_from_backup)?;
        write_property(bytes, 303, &self.rebalance_batch_size)?;
        write_property(bytes, 304, &self.rebalance_batch_prefetch_count)?;
        write_property(bytes, 301, &self.rebalance_delay)?;
        write_property(bytes, 300, &self.rebalance_mode)?;
        write_property(bytes, 305, &self.rebalance_order)?;
        write_property(bytes, 306, &self.rebalance_throttle)?;
        write_property(bytes, 302, &self.rebalance_timeout)?;
        write_property(bytes, 205, &self.sql_escape_all)?;
        write_property(bytes, 204, &self.sql_index_inline_max_size)?;
        write_property(bytes, 203, &self.sql_schema)?;
        write_property(bytes, 4, &self.write_synchronization_mode)?;
        write_property(bytes, 401, &self.cache_key_configurations)?;
        write_property(bytes, 200, &self.query_entities)?;

        bytes.put_i32_le(config_bytes.len() as i32);
        bytes.put_i16_le(30);
        bytes.put(config_bytes);

        Ok(())
    }
}

fn write_property<T: IgniteWrite>(bytes: &mut BytesMut, code: i16, value: &T) -> Result<()> {
    bytes.put_i16(code);

    value.write(bytes)
}
