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
    pub(crate) type_name: String,
    pub(crate) affinity_key_field_name: String,
}

impl CacheKeyConfiguration {
    pub fn new(type_name: &str, affinity_key_field_name: &str) -> CacheKeyConfiguration {
        CacheKeyConfiguration {
            type_name: type_name.to_string(),
            affinity_key_field_name: affinity_key_field_name.to_string(),
        }
    }
}

#[derive(IgniteRead, IgniteWrite)]
pub struct QueryField {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) key_field: bool,
    pub(crate) not_null: bool,
    pub(crate) default_value: Option<Value>,
}

impl QueryField {
    pub fn new(name: &str, type_name: &str, key_field: bool, not_null: bool) -> QueryField {
        QueryField {
            name: name.to_string(),
            type_name: type_name.to_string(),
            key_field,
            not_null,
            default_value: None,
        }
    }

    pub fn default_value(mut self, default_value: Value) -> QueryField {
        self.default_value = Some(default_value);

        self
    }
}

#[derive(IgniteRead, IgniteWrite)]
pub struct QueryIndex {
    pub(crate) index_name: String,
    pub(crate) index_type: IndexType,
    pub(crate) inline_size: i32,
    pub(crate) fields: Vec<(String, bool)>,
}

impl QueryIndex {
    pub fn new(index_name: &str, index_type: IndexType) -> QueryIndex {
        QueryIndex {
            index_name: index_name.to_string(),
            index_type,
            inline_size: 10,
            fields: Vec::new(),
        }
    }

    pub fn field(mut self, name: &str, desc: bool) -> QueryIndex {
        self.fields.push((name.to_string(), desc));

        self
    }
}

#[derive(IgniteRead, IgniteWrite)]
pub struct QueryEntity {
    pub(crate) key_type_name: String,
    pub(crate) value_type_name: String,
    pub(crate) table_name: String,
    pub(crate) key_field_name: Option<String>,
    pub(crate) value_field_name: Option<String>,
    pub(crate) fields: Vec<QueryField>,
    pub(crate) aliases: Vec<(String, String)>,
    pub(crate) indexes: Vec<QueryIndex>,
}

impl QueryEntity {
    pub fn new(key_type_name: &str, value_type_name: &str, table_name: &str) -> QueryEntity {
        QueryEntity {
            key_type_name: key_type_name.to_string(),
            value_type_name: value_type_name.to_string(),
            table_name: table_name.to_string(),
            key_field_name: None,
            value_field_name: None,
            fields: Vec::new(),
            aliases: Vec::new(),
            indexes: Vec::new(),
        }
    }

    pub fn key_field_name(mut self, key_field_name: &str) -> QueryEntity {
        self.key_field_name = Some(key_field_name.to_string());

        self
    }

    pub fn value_field_name(mut self, value_field_name: &str) -> QueryEntity {
        self.value_field_name = Some(value_field_name.to_string());

        self
    }

    pub fn field(mut self, field: QueryField) -> QueryEntity {
        self.fields.push(field);

        self
    }

    pub fn alias(mut self, field_name: &str, alias: &str) -> QueryEntity {
        self.aliases.push((field_name.to_string(), alias.to_string()));

        self
    }

    pub fn index(mut self, index: QueryIndex) -> QueryEntity {
        self.indexes.push(index);

        self
    }
}

#[derive(IgniteRead)]
pub struct CacheConfiguration {
    pub(crate) atomicity_mode: AtomicityMode,
    pub(crate) backups: i32,
    pub(crate) mode: CacheMode,
    pub(crate) copy_on_read: bool,
    pub(crate) data_region_name: Option<String>,
    pub(crate) eager_ttl: bool,
    pub(crate) statistics_enabled: bool,
    pub(crate) group_name: Option<String>,
    pub(crate) default_lock_timeout: i64,
    pub(crate) max_concurrent_async_operations: i32,
    pub(crate) max_query_iterators: i32,
    pub(crate) name: String,
    pub(crate) on_heap_cache_enabled: bool,
    pub(crate) partition_loss_policy: PartitionLossPolicy,
    pub(crate) query_detail_metrics_size: i32,
    pub(crate) query_parallelism: i32,
    pub(crate) read_from_backup: bool,
    pub(crate) rebalance_batch_size: i32,
    pub(crate) rebalance_batch_prefetch_count: i64,
    pub(crate) rebalance_delay: i64,
    pub(crate) rebalance_mode: RebalanceMode,
    pub(crate) rebalance_order: i32,
    pub(crate) rebalance_throttle: i64,
    pub(crate) rebalance_timeout: i64,
    pub(crate) sql_escape_all: bool,
    pub(crate) sql_index_inline_max_size: i32,
    pub(crate) sql_schema: Option<String>,
    pub(crate) write_synchronization_mode: WriteSynchronizationMode,
    pub(crate) cache_key_configurations: Vec<CacheKeyConfiguration>,
    pub(crate) query_entities: Vec<QueryEntity>,
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

    pub fn atomicity_mode(mut self, atomicity_mode: AtomicityMode) -> CacheConfiguration {
        self.atomicity_mode = atomicity_mode;

        self
    }

    pub fn backups(mut self, backups: i32) -> CacheConfiguration {
        self.backups = backups;

        self
    }

    pub fn mode(mut self, mode: CacheMode) -> CacheConfiguration {
        self.mode = mode;

        self
    }

    pub fn copy_on_read(mut self, copy_on_read: bool) -> CacheConfiguration {
        self.copy_on_read = copy_on_read;

        self
    }

    pub fn data_region_name(mut self, data_region_name: &str) -> CacheConfiguration {
        self.data_region_name = Some(data_region_name.to_string());

        self
    }

    pub fn eager_ttl(mut self, eager_ttl: bool) -> CacheConfiguration {
        self.eager_ttl = eager_ttl;

        self
    }

    pub fn statistics_enabled(mut self, statistics_enabled: bool) -> CacheConfiguration {
        self.statistics_enabled = statistics_enabled;

        self
    }

    pub fn group_name(mut self, group_name: &str) -> CacheConfiguration {
        self.group_name = Some(group_name.to_string());

        self
    }

    pub fn default_lock_timeout(mut self, default_lock_timeout: i64) -> CacheConfiguration {
        self.default_lock_timeout = default_lock_timeout;

        self
    }

    pub fn max_concurrent_async_operations(mut self, max_concurrent_async_operations: i32) -> CacheConfiguration {
        self.max_concurrent_async_operations = max_concurrent_async_operations;

        self
    }

    pub fn max_query_iterators(mut self, max_query_iterators: i32) -> CacheConfiguration {
        self.max_query_iterators = max_query_iterators;

        self
    }

    pub fn on_heap_cache_enabled(mut self, on_heap_cache_enabled: bool) -> CacheConfiguration {
        self.on_heap_cache_enabled = on_heap_cache_enabled;

        self
    }

    pub fn partition_loss_policy(mut self, partition_loss_policy: PartitionLossPolicy) -> CacheConfiguration {
        self.partition_loss_policy = partition_loss_policy;

        self
    }

    pub fn query_detail_metrics_size(mut self, query_detail_metrics_size: i32) -> CacheConfiguration {
        self.query_detail_metrics_size = query_detail_metrics_size;

        self
    }

    pub fn query_parallelism(mut self, query_parallelism: i32) -> CacheConfiguration {
        self.query_parallelism = query_parallelism;

        self
    }

    pub fn read_from_backup(mut self, read_from_backup: bool) -> CacheConfiguration {
        self.read_from_backup = read_from_backup;

        self
    }

    pub fn rebalance_batch_size(mut self, rebalance_batch_size: i32) -> CacheConfiguration {
        self.rebalance_batch_size = rebalance_batch_size;

        self
    }

    pub fn rebalance_batch_prefetch_count(mut self, rebalance_batch_prefetch_count: i64) -> CacheConfiguration {
        self.rebalance_batch_prefetch_count = rebalance_batch_prefetch_count;

        self
    }

    pub fn rebalance_delay(mut self, rebalance_delay: i64) -> CacheConfiguration {
        self.rebalance_delay = rebalance_delay;

        self
    }

    pub fn rebalance_mode(mut self, rebalance_mode: RebalanceMode) -> CacheConfiguration {
        self.rebalance_mode = rebalance_mode;

        self
    }

    pub fn rebalance_order(mut self, rebalance_order: i32) -> CacheConfiguration {
        self.rebalance_order = rebalance_order;

        self
    }

    pub fn rebalance_throttle(mut self, rebalance_throttle: i64) -> CacheConfiguration {
        self.rebalance_throttle = rebalance_throttle;

        self
    }

    pub fn rebalance_timeout(mut self, rebalance_timeout: i64) -> CacheConfiguration {
        self.rebalance_timeout = rebalance_timeout;

        self
    }

    pub fn sql_escape_all(mut self, sql_escape_all: bool) -> CacheConfiguration {
        self.sql_escape_all = sql_escape_all;

        self
    }

    pub fn sql_index_inline_max_size(mut self, sql_index_inline_max_size: i32) -> CacheConfiguration {
        self.sql_index_inline_max_size = sql_index_inline_max_size;

        self
    }

    pub fn sql_schema(mut self, sql_schema: &str) -> CacheConfiguration {
        self.sql_schema = Some(sql_schema.to_string());

        self
    }

    pub fn write_synchronization_mode(mut self, write_synchronization_mode: WriteSynchronizationMode) -> CacheConfiguration {
        self.write_synchronization_mode = write_synchronization_mode;

        self
    }

    pub fn cache_key_configuration(mut self, cache_key_configuration: CacheKeyConfiguration) -> CacheConfiguration {
        self.cache_key_configurations.push(cache_key_configuration);

        self
    }

    pub fn query_entity(mut self, query_entity: QueryEntity) -> CacheConfiguration {
        self.query_entities.push(query_entity);

        self
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
