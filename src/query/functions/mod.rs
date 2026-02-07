//! Custom User-Defined Functions (UDFs) and Aggregates (UDAFs)
//!
//! This module provides custom functions for the query engine:
//! - `time_bucket` - Time bucketing for GROUP BY time() queries (UDF)
//! - `first_value` - Returns value with minimum timestamp (UDAF)
//! - `last_value` - Returns value with maximum timestamp (UDAF)

pub mod first_last;
pub mod geospatial;
pub mod time_bucket;

pub use first_last::{create_first_value_udaf, create_last_value_udaf};
pub use geospatial::{
    create_geo_bearing_udf, create_geo_bounding_box_udf, create_geo_distance_udf,
    create_geo_hash_decode_lat_udf, create_geo_hash_decode_lon_udf, create_geo_hash_neighbors_udf,
    create_geo_hash_udf, create_geo_within_udf,
};
pub use time_bucket::create_time_bucket_udf;
