use bytes::{Bytes, Buf};
use num_traits::{FromPrimitive, ToPrimitive};
use crate::binary::Value;

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
pub enum CacheMode {
    Local = 0,
    Replicated = 1,
    Partitioned = 2,
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum PartitionLossPolicy {
    ReadOnlySafe = 0,
    ReadOnlyAll = 1,
    ReadWriteSafe = 2,
    ReadWriteAll = 3,
    Ignore = 4,
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum RebalanceMode {
    Sync = 0,
    Async = 1,
    None = 2,
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum WriteSynchronizationMode {
    FullSync = 0,
    FullAsync = 1,
    PrimarySync = 2,
}

pub struct CacheKeyConfiguration {
    type_name: String,
    affinity_key_field_name: String,
}

pub struct QueryEntity {
    key_type_name: String,
    value_type_name: String,
    table_name: String,
    key_field_name: String,
    value_field_name: String,
    fields: Vec<QueryField>,
    indexes: Vec<QueryIndex>,
}

pub struct QueryField {
    name: String,
    type_name: String,
    key_field: bool,
    not_null: bool,
}

pub enum IndexType {
    Sorted = 0,
    FullText = 1,
    Geospatial = 2,
}

pub struct QueryIndex {
    index_name: String,
    index_type: IndexType,
    inline_size: i32,
    fields: Vec<(String, bool)>,
}

pub struct CacheConfiguration {
    backups: i32,
    mode: CacheMode,
    copy_on_read: bool,
    data_region_name: Option<String>,
    eager_ttl: bool,
    statistics_enabled: bool,
    group_name: Option<String>,
    invalidate: bool,
    default_lock_timeout: i64,
    max_query_iterators: i32,
    name: Option<String>,
    on_heap_cache_enabled: bool,
    partition_loss_policy: PartitionLossPolicy,
    query_detail_metrics_size: i32,
    query_parallelism: i32,
    read_from_backup: bool,
    rebalance_batch_size: i32,
    rebalance_batch_prefetch_count: i64,
    rebalance_delay: i64,
    rebalance_mode: RebalanceMode,
    rebalance_order: i32,
    rebalance_throttle: i64,
    rebalance_timeout: i64,
    sql_escape_all: bool,
    sql_index_inline_max_size: i32,
    sql_schema: Option<String>,
    write_synchronization_mode: WriteSynchronizationMode,
    cache_key_configurations: Vec<CacheKeyConfiguration>,
    query_entities: Vec<QueryEntity>,
    aliases: Vec<(String, String)>,
}

// impl CacheConfiguration {
//     pub(crate) fn read(bytes: &mut Bytes) -> Result<CacheConfiguration> {
//         bytes.advance(4); // Ignore length.
//
//         Ok(CacheConfiguration {
//             backups: bytes.get_i32_le(),
//             mode: FromPrimitive::from_i32(bytes.get_i32_le())?,
//             copy_on_read: bytes.get_u8() != 0,
//             data_region_name: Value::read(bytes)?.map(|value| { if String(value) = value { value } else "" }),
//         })
//     }
// }
