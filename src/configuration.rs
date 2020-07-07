use bytes::{Bytes, Buf};
use num_traits::{FromPrimitive, ToPrimitive};

use crate::binary;
use crate::error::{Result, Error, ErrorKind};

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

impl CacheMode {
    fn read(bytes: &mut Bytes) -> Result<CacheMode> {
        let mode: Option<CacheMode> = FromPrimitive::from_i32(binary::read_i32_with_type_check(bytes)?);

        match mode {
            Some(mode) => Ok(mode),
            None => Err(Error::new(ErrorKind::Serde, "".to_string())),
        }
    }
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum PartitionLossPolicy {
    ReadOnlySafe = 0,
    ReadOnlyAll = 1,
    ReadWriteSafe = 2,
    ReadWriteAll = 3,
    Ignore = 4,
}

impl PartitionLossPolicy {
    fn read(bytes: &mut Bytes) -> Result<PartitionLossPolicy> {
        let mode: Option<PartitionLossPolicy> = FromPrimitive::from_i32(binary::read_i32_with_type_check(bytes)?);

        match mode {
            Some(mode) => Ok(mode),
            None => Err(Error::new(ErrorKind::Serde, "".to_string())),
        }
    }
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum RebalanceMode {
    Sync = 0,
    Async = 1,
    None = 2,
}

impl RebalanceMode {
    fn read(bytes: &mut Bytes) -> Result<RebalanceMode> {
        let mode: Option<RebalanceMode> = FromPrimitive::from_i32(binary::read_i32_with_type_check(bytes)?);

        match mode {
            Some(mode) => Ok(mode),
            None => Err(Error::new(ErrorKind::Serde, "".to_string())),
        }
    }
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum WriteSynchronizationMode {
    FullSync = 0,
    FullAsync = 1,
    PrimarySync = 2,
}

impl WriteSynchronizationMode {
    fn read(bytes: &mut Bytes) -> Result<WriteSynchronizationMode> {
        let mode: Option<WriteSynchronizationMode> = FromPrimitive::from_i32(binary::read_i32_with_type_check(bytes)?);

        match mode {
            Some(mode) => Ok(mode),
            None => Err(Error::new(ErrorKind::Serde, "".to_string())),
        }
    }
}

pub struct CacheKeyConfiguration {
    pub type_name: String,
    pub affinity_key_field_name: String,
}

pub struct QueryEntity {
    pub key_type_name: String,
    pub value_type_name: String,
    pub table_name: String,
    pub key_field_name: String,
    pub value_field_name: String,
    pub fields: Vec<QueryField>,
    pub indexes: Vec<QueryIndex>,
}

pub struct QueryField {
    pub name: String,
    pub type_name: String,
    pub key_field: bool,
    pub not_null: bool,
}

pub enum IndexType {
    Sorted = 0,
    FullText = 1,
    Geospatial = 2,
}

pub struct QueryIndex {
    pub index_name: String,
    pub index_type: IndexType,
    pub inline_size: i32,
    pub fields: Vec<(String, bool)>,
}

pub struct CacheConfiguration {
    pub backups: i32,
    pub mode: CacheMode,
    pub copy_on_read: bool,
    pub data_region_name: Option<String>,
    pub eager_ttl: bool,
    pub statistics_enabled: bool,
    pub group_name: Option<String>,
    pub invalidate: bool,
    pub default_lock_timeout: i64,
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
    pub aliases: Vec<(String, String)>,
}

impl CacheConfiguration {
    pub(crate) fn read(bytes: &mut Bytes) -> Result<CacheConfiguration> {
        bytes.advance(4); // Ignore length.

        Ok(CacheConfiguration {
            backups: binary::read_i32_with_type_check(bytes)?,
            mode: CacheMode::read(bytes)?,
            copy_on_read: binary::read_bool_with_type_check(bytes)?,
            data_region_name: binary::read_string_optional_with_type_check(bytes)?,
            eager_ttl: binary::read_bool_with_type_check(bytes)?,
            statistics_enabled: binary::read_bool_with_type_check(bytes)?,
            group_name: binary::read_string_optional_with_type_check(bytes)?,
            invalidate: binary::read_bool_with_type_check(bytes)?,
            default_lock_timeout: binary::read_i64_with_type_check(bytes)?,
            max_query_iterators: binary::read_i32_with_type_check(bytes)?,
            name: binary::read_string_optional_with_type_check(bytes)?,
            on_heap_cache_enabled: binary::read_bool_with_type_check(bytes)?,
            partition_loss_policy: PartitionLossPolicy::read(bytes)?,
            query_detail_metrics_size: binary::read_i32_with_type_check(bytes)?,
            query_parallelism: binary::read_i32_with_type_check(bytes)?,
            read_from_backup: binary::read_bool_with_type_check(bytes)?,
            rebalance_batch_size: binary::read_i32_with_type_check(bytes)?,
            rebalance_batch_prefetch_count: binary::read_i64_with_type_check(bytes)?,
            rebalance_delay: binary::read_i64_with_type_check(bytes)?,
            rebalance_mode: RebalanceMode::read(bytes)?,
            rebalance_order: binary::read_i32_with_type_check(bytes)?,
            rebalance_throttle: binary::read_i64_with_type_check(bytes)?,
            rebalance_timeout: binary::read_i64_with_type_check(bytes)?,
            sql_escape_all: binary::read_bool_with_type_check(bytes)?,
            sql_index_inline_max_size: binary::read_i32_with_type_check(bytes)?,
            sql_schema: binary::read_string_optional_with_type_check(bytes)?,
            write_synchronization_mode: WriteSynchronizationMode::read(bytes)?,
            // TODO
            cache_key_configurations: vec![],
            // TODO
            query_entities: vec![],
            // TODO
            aliases: vec![],
        })
    }
}
