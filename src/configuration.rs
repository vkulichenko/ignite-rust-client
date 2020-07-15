use bytes::{Bytes, Buf};

use crate::error::Result;
use crate::binary::{Read, EnumRead, Value};

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

#[derive(FromPrimitive, ToPrimitive)]
pub enum AtomicityMode {
    Transactional = 0,
    Atomic = 1,
    TransactionalSnapshot = 2,
}

impl EnumRead for AtomicityMode {}

#[derive(FromPrimitive, ToPrimitive)]
pub enum CacheMode {
    Local = 0,
    Replicated = 1,
    Partitioned = 2,
}

impl EnumRead for CacheMode {}

#[derive(FromPrimitive, ToPrimitive)]
pub enum PartitionLossPolicy {
    ReadOnlySafe = 0,
    ReadOnlyAll = 1,
    ReadWriteSafe = 2,
    ReadWriteAll = 3,
    Ignore = 4,
}

impl EnumRead for PartitionLossPolicy {}

#[derive(FromPrimitive, ToPrimitive)]
pub enum RebalanceMode {
    Sync = 0,
    Async = 1,
    None = 2,
}

impl EnumRead for RebalanceMode {}

#[derive(FromPrimitive, ToPrimitive)]
pub enum WriteSynchronizationMode {
    FullSync = 0,
    FullAsync = 1,
    PrimarySync = 2,
}

impl EnumRead for WriteSynchronizationMode {}

#[derive(FromPrimitive, ToPrimitive)]
pub enum IndexType {
    Sorted = 0,
    FullText = 1,
    Geospatial = 2,
}

impl EnumRead for IndexType {}

pub struct CacheKeyConfiguration {
    pub type_name: String,
    pub affinity_key_field_name: String,
}

impl Read for CacheKeyConfiguration {
    fn read(bytes: &mut Bytes) -> Result<CacheKeyConfiguration> {
        Ok(CacheKeyConfiguration {
            type_name: Read::read(bytes)?,
            affinity_key_field_name: Read::read(bytes)?,
        })
    }
}

pub struct QueryField {
    pub name: String,
    pub type_name: String,
    pub key_field: bool,
    pub not_null: bool,
    pub default_value: Option<Value>,
}

impl Read for QueryField {
    fn read(bytes: &mut Bytes) -> Result<QueryField> {
        Ok(QueryField {
            name: Read::read(bytes)?,
            type_name: Read::read(bytes)?,
            key_field: Read::read(bytes)?,
            not_null: Read::read(bytes)?,
            default_value: Value::read(bytes)?,
        })
    }
}

pub struct QueryIndex {
    pub index_name: String,
    pub index_type: IndexType,
    pub inline_size: i32,
    pub fields: Vec<(String, bool)>,
}

impl Read for QueryIndex {
    fn read(bytes: &mut Bytes) -> Result<QueryIndex> {
        Ok(QueryIndex {
            index_name: Read::read(bytes)?,
            index_type: Read::read(bytes)?,
            inline_size: Read::read(bytes)?,
            fields: Read::read(bytes)?,
        })
    }
}

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

impl Read for QueryEntity {
    fn read(bytes: &mut Bytes) -> Result<QueryEntity> {
        Ok(QueryEntity {
            key_type_name: Read::read(bytes)?,
            value_type_name: Read::read(bytes)?,
            table_name: Read::read(bytes)?,
            key_field_name: Read::read(bytes)?,
            value_field_name: Read::read(bytes)?,
            fields: Read::read(bytes)?,
            aliases: Read::read(bytes)?,
            indexes: Read::read(bytes)?,
        })
    }
}

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
    pub name: Option<String>,
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
    pub(crate) fn read(bytes: &mut Bytes) -> Result<CacheConfiguration> {
        bytes.advance(4); // Ignore length.

        Ok(CacheConfiguration {
            atomicity_mode: Read::read(bytes)?,
            backups: Read::read(bytes)?,
            mode: Read::read(bytes)?,
            copy_on_read: Read::read(bytes)?,
            data_region_name: Read::read(bytes)?,
            eager_ttl: Read::read(bytes)?,
            statistics_enabled: Read::read(bytes)?,
            group_name: Read::read(bytes)?,
            default_lock_timeout: Read::read(bytes)?,
            max_concurrent_async_operations: Read::read(bytes)?,
            max_query_iterators: Read::read(bytes)?,
            name: Read::read(bytes)?,
            on_heap_cache_enabled: Read::read(bytes)?,
            partition_loss_policy: Read::read(bytes)?,
            query_detail_metrics_size: Read::read(bytes)?,
            query_parallelism: Read::read(bytes)?,
            read_from_backup: Read::read(bytes)?,
            rebalance_batch_size: Read::read(bytes)?,
            rebalance_batch_prefetch_count: Read::read(bytes)?,
            rebalance_delay: Read::read(bytes)?,
            rebalance_mode: Read::read(bytes)?,
            rebalance_order: Read::read(bytes)?,
            rebalance_throttle: Read::read(bytes)?,
            rebalance_timeout: Read::read(bytes)?,
            sql_escape_all: Read::read(bytes)?,
            sql_index_inline_max_size: Read::read(bytes)?,
            sql_schema: Read::read(bytes)?,
            write_synchronization_mode: Read::read(bytes)?,
            cache_key_configurations: Read::read(bytes)?,
            query_entities: Read::read(bytes)?,
        })
    }
}
