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
        let mode: Option<CacheMode> = FromPrimitive::from_i32(binary::read(bytes)?);

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
        let mode: Option<PartitionLossPolicy> = FromPrimitive::from_i32(binary::read(bytes)?);

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
        let mode: Option<RebalanceMode> = FromPrimitive::from_i32(binary::read(bytes)?);

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
        let mode: Option<WriteSynchronizationMode> = FromPrimitive::from_i32(binary::read(bytes)?);

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

impl CacheKeyConfiguration {
    fn read(bytes: &mut Bytes) -> Result<CacheKeyConfiguration> {
        Ok(CacheKeyConfiguration {
            type_name: binary::read(bytes)?,
            affinity_key_field_name: binary::read(bytes)?,
        })
    }

    fn read_multiple(bytes: &mut Bytes) -> Result<Vec<CacheKeyConfiguration>> {
        let len = bytes.get_i32_le() as usize;

        let mut vec = Vec::with_capacity(len);

        for _ in 0 .. len {
            vec.push(CacheKeyConfiguration::read(bytes)?);
        }

        Ok(vec)
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

impl QueryEntity {
    fn read(bytes: &mut Bytes) -> Result<QueryEntity> {
        fn read_aliases(bytes: &mut Bytes) -> Result<Vec<(String, String)>> {
            let len = bytes.get_i32_le() as usize;

            let mut vec = Vec::with_capacity(len);

            for _ in 0 .. len {
                let name = binary::read(bytes)?;
                let alias = binary::read(bytes)?;

                vec.push((name, alias));
            }

            Ok(vec)
        }

        Ok(QueryEntity {
            key_type_name: binary::read(bytes)?,
            value_type_name: binary::read(bytes)?,
            table_name: binary::read(bytes)?,
            key_field_name: binary::read(bytes)?,
            value_field_name: binary::read(bytes)?,
            fields: QueryField::read_multiple(bytes)?,
            aliases: read_aliases(bytes)?,
            indexes: QueryIndex::read_multiple(bytes)?,
        })
    }

    fn read_multiple(bytes: &mut Bytes) -> Result<Vec<QueryEntity>> {
        let len = bytes.get_i32_le() as usize;

        let mut vec = Vec::with_capacity(len);

        for _ in 0 .. len {
            vec.push(QueryEntity::read(bytes)?);
        }

        Ok(vec)
    }
}

pub struct QueryField {
    pub name: String,
    pub type_name: String,
    pub key_field: bool,
    pub not_null: bool,
}

impl QueryField {
    fn read(bytes: &mut Bytes) -> Result<QueryField> {
        Ok(QueryField {
            name: binary::read(bytes)?,
            type_name: binary::read(bytes)?,
            key_field: binary::read(bytes)?,
            not_null: binary::read(bytes)?,
        })
    }

    fn read_multiple(bytes: &mut Bytes) -> Result<Vec<QueryField>> {
        let len = bytes.get_i32_le() as usize;

        let mut vec = Vec::with_capacity(len);

        for _ in 0 .. len {
            vec.push(QueryField::read(bytes)?);
        }

        Ok(vec)
    }
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum IndexType {
    Sorted = 0,
    FullText = 1,
    Geospatial = 2,
}

impl IndexType {
    fn read(bytes: &mut Bytes) -> Result<IndexType> {
        let mode: Option<IndexType> = FromPrimitive::from_i32(binary::read(bytes)?);

        match mode {
            Some(mode) => Ok(mode),
            None => Err(Error::new(ErrorKind::Serde, "".to_string())),
        }
    }
}

pub struct QueryIndex {
    pub index_name: String,
    pub index_type: IndexType,
    pub inline_size: i32,
    pub fields: Vec<(String, bool)>,
}

impl QueryIndex {
    fn read(bytes: &mut Bytes) -> Result<QueryIndex> {
        fn read_fields(bytes: &mut Bytes) -> Result<Vec<(String, bool)>> {
            let len = bytes.get_i32_le() as usize;

            let mut vec = Vec::with_capacity(len);

            for _ in 0 .. len {
                let name = binary::read(bytes)?;
                let desc = binary::read(bytes)?;

                vec.push((name, desc));
            }

            Ok(vec)
        }

        Ok(QueryIndex {
            index_name: binary::read(bytes)?,
            index_type: IndexType::read(bytes)?,
            inline_size: binary::read(bytes)?,
            fields: read_fields(bytes)?,
        })
    }

    fn read_multiple(bytes: &mut Bytes) -> Result<Vec<QueryIndex>> {
        let len = bytes.get_i32_le() as usize;

        let mut vec = Vec::with_capacity(len);

        for _ in 0 .. len {
            vec.push(QueryIndex::read(bytes)?);
        }

        Ok(vec)
    }
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
}

impl CacheConfiguration {
    pub(crate) fn read(bytes: &mut Bytes) -> Result<CacheConfiguration> {
        bytes.advance(4); // Ignore length.

        Ok(CacheConfiguration {
            backups: binary::read(bytes)?,
            mode: CacheMode::read(bytes)?,
            copy_on_read: binary::read(bytes)?,
            data_region_name: binary::read_optional(bytes)?,
            eager_ttl: binary::read(bytes)?,
            statistics_enabled: binary::read(bytes)?,
            group_name: binary::read_optional(bytes)?,
            invalidate: binary::read(bytes)?,
            default_lock_timeout: binary::read(bytes)?,
            max_query_iterators: binary::read(bytes)?,
            name: binary::read_optional(bytes)?,
            on_heap_cache_enabled: binary::read(bytes)?,
            partition_loss_policy: PartitionLossPolicy::read(bytes)?,
            query_detail_metrics_size: binary::read(bytes)?,
            query_parallelism: binary::read(bytes)?,
            read_from_backup: binary::read(bytes)?,
            rebalance_batch_size: binary::read(bytes)?,
            rebalance_batch_prefetch_count: binary::read(bytes)?,
            rebalance_delay: binary::read(bytes)?,
            rebalance_mode: RebalanceMode::read(bytes)?,
            rebalance_order: binary::read(bytes)?,
            rebalance_throttle: binary::read(bytes)?,
            rebalance_timeout: binary::read(bytes)?,
            sql_escape_all: binary::read(bytes)?,
            sql_index_inline_max_size: binary::read(bytes)?,
            sql_schema: binary::read_optional(bytes)?,
            write_synchronization_mode: WriteSynchronizationMode::read(bytes)?,
            cache_key_configurations: CacheKeyConfiguration::read_multiple(bytes)?,
            query_entities: QueryEntity::read_multiple(bytes)?,
        })
    }
}
