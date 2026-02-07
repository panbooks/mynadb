//! Geospatial UDFs
//!
//! PostGIS-like scalar functions for geospatial queries on lat/lon Float64 columns.
//!
//! Functions:
//! - `geo_distance(lat1, lon1, lat2, lon2)` → Float64 meters (Haversine)
//! - `geo_hash(lat, lon, precision)` → Utf8 geohash string
//! - `geo_hash_decode_lat(geohash)` → Float64 latitude
//! - `geo_hash_decode_lon(geohash)` → Float64 longitude
//! - `geo_hash_neighbors(geohash)` → Utf8 comma-separated 8 neighbors
//! - `geo_within(lat, lon, center_lat, center_lon, radius_m)` → Boolean
//! - `geo_bounding_box(lat, lon, min_lat, min_lon, max_lat, max_lon)` → Boolean
//! - `geo_bearing(lat1, lon1, lat2, lon2)` → Float64 degrees [0, 360)
//!
//! All functions use `(lat, lon)` argument order. Internally, `geo::Point::new(x, y)`
//! takes `(longitude, latitude)`, so arguments are swapped when constructing points.

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use geo::algorithm::bearing::HaversineBearing;
use geo::algorithm::haversine_distance::HaversineDistance;
use geo::Point;
use std::any::Any;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Extract f64 from a ScalarValue.
fn scalar_to_f64(scalar: &ScalarValue) -> Result<f64> {
    match scalar {
        ScalarValue::Float64(Some(v)) => Ok(*v),
        ScalarValue::Float32(Some(v)) => Ok(*v as f64),
        ScalarValue::Int64(Some(v)) => Ok(*v as f64),
        ScalarValue::Int32(Some(v)) => Ok(*v as f64),
        _ => Err(DataFusionError::Execution(format!(
            "geo: expected numeric value, got {:?}",
            scalar
        ))),
    }
}

/// Extract String from a ScalarValue.
fn scalar_to_string(scalar: &ScalarValue) -> Result<String> {
    match scalar {
        ScalarValue::Utf8(Some(v)) => Ok(v.clone()),
        ScalarValue::LargeUtf8(Some(v)) => Ok(v.clone()),
        _ => Err(DataFusionError::Execution(format!(
            "geo: expected string value, got {:?}",
            scalar
        ))),
    }
}

/// Convert ColumnarValue to ArrayRef, expanding scalars to the given length.
fn columnar_to_array(cv: &ColumnarValue, len: usize) -> Result<ArrayRef> {
    match cv {
        ColumnarValue::Array(arr) => Ok(arr.clone()),
        ColumnarValue::Scalar(scalar) => Ok(scalar.to_array_of_size(len)?),
    }
}

/// Find array length from a slice of ColumnarValues.
fn batch_length(args: &[ColumnarValue]) -> usize {
    args.iter()
        .find_map(|cv| match cv {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .unwrap_or(1)
}

// ---------------------------------------------------------------------------
// geo_distance(lat1, lon1, lat2, lon2) → Float64 meters
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GeoDistanceUDF {
    signature: Signature,
}

impl GeoDistanceUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeoDistanceUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geo_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 4 {
            return Err(DataFusionError::Execution(
                "geo_distance requires 4 arguments: lat1, lon1, lat2, lon2".to_string(),
            ));
        }

        // All-scalar fast path
        if let (
            ColumnarValue::Scalar(s0),
            ColumnarValue::Scalar(s1),
            ColumnarValue::Scalar(s2),
            ColumnarValue::Scalar(s3),
        ) = (&args[0], &args[1], &args[2], &args[3])
        {
            if s0.is_null() || s1.is_null() || s2.is_null() || s3.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
            }
            let (lat1, lon1, lat2, lon2) = (
                scalar_to_f64(s0)?,
                scalar_to_f64(s1)?,
                scalar_to_f64(s2)?,
                scalar_to_f64(s3)?,
            );
            let d = Point::new(lon1, lat1).haversine_distance(&Point::new(lon2, lat2));
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(d))));
        }

        // Array path
        let len = batch_length(&args);
        let arrs: Vec<ArrayRef> = args.iter().map(|a| columnar_to_array(a, len)).collect::<Result<_>>()?;
        let lat1 = arrs[0].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_distance: lat1 must be Float64".into())
        })?;
        let lon1 = arrs[1].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_distance: lon1 must be Float64".into())
        })?;
        let lat2 = arrs[2].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_distance: lat2 must be Float64".into())
        })?;
        let lon2 = arrs[3].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_distance: lon2 must be Float64".into())
        })?;

        let result: Float64Array = (0..len)
            .map(|i| {
                if lat1.is_null(i) || lon1.is_null(i) || lat2.is_null(i) || lon2.is_null(i) {
                    None
                } else {
                    let d = Point::new(lon1.value(i), lat1.value(i))
                        .haversine_distance(&Point::new(lon2.value(i), lat2.value(i)));
                    Some(d)
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn create_geo_distance_udf() -> ScalarUDF {
    ScalarUDF::from(GeoDistanceUDF::new())
}

// ---------------------------------------------------------------------------
// geo_hash(lat, lon, precision) → Utf8
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GeoHashUDF {
    signature: Signature,
}

impl GeoHashUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Int64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeoHashUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geo_hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return Err(DataFusionError::Execution(
                "geo_hash requires 3 arguments: lat, lon, precision".to_string(),
            ));
        }

        // All-scalar fast path
        if let (
            ColumnarValue::Scalar(s0),
            ColumnarValue::Scalar(s1),
            ColumnarValue::Scalar(s2),
        ) = (&args[0], &args[1], &args[2])
        {
            if s0.is_null() || s1.is_null() || s2.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
            }
            let (lat, lon) = (scalar_to_f64(s0)?, scalar_to_f64(s1)?);
            let precision = scalar_to_f64(s2)? as usize;
            match geohash::encode(geohash::Coord { x: lon, y: lat }, precision) {
                Ok(h) => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(h)))),
                Err(_) => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            }
        }

        // Array path
        let len = batch_length(&args);
        let arrs: Vec<ArrayRef> = args.iter().map(|a| columnar_to_array(a, len)).collect::<Result<_>>()?;
        let lat_arr = arrs[0].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_hash: lat must be Float64".into())
        })?;
        let lon_arr = arrs[1].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_hash: lon must be Float64".into())
        })?;
        // precision can be Int64 or Float64 (from scalar expansion)
        let prec_arr = &arrs[2];

        let result: StringArray = (0..len)
            .map(|i| {
                if lat_arr.is_null(i) || lon_arr.is_null(i) || prec_arr.is_null(i) {
                    None
                } else {
                    let precision = if let Some(a) = prec_arr.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                        a.value(i) as usize
                    } else if let Some(a) = prec_arr.as_any().downcast_ref::<Float64Array>() {
                        a.value(i) as usize
                    } else {
                        return None;
                    };
                    geohash::encode(
                        geohash::Coord {
                            x: lon_arr.value(i),
                            y: lat_arr.value(i),
                        },
                        precision,
                    )
                    .ok()
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn create_geo_hash_udf() -> ScalarUDF {
    ScalarUDF::from(GeoHashUDF::new())
}

// ---------------------------------------------------------------------------
// geo_hash_decode_lat(geohash) → Float64
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GeoHashDecodeLatUDF {
    signature: Signature,
}

impl GeoHashDecodeLatUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeoHashDecodeLatUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geo_hash_decode_lat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(DataFusionError::Execution(
                "geo_hash_decode_lat requires 1 argument: geohash".to_string(),
            ));
        }

        // Scalar fast path
        if let ColumnarValue::Scalar(s) = &args[0] {
            if s.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
            }
            let hash = scalar_to_string(s)?;
            return match geohash::decode(&hash) {
                Ok((coord, _, _)) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(coord.y)))),
                Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(None))),
            };
        }

        // Array path
        let len = batch_length(&args);
        let arr = columnar_to_array(&args[0], len)?;
        let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Execution("geo_hash_decode_lat: argument must be Utf8".into())
        })?;

        let result: Float64Array = (0..len)
            .map(|i| {
                if str_arr.is_null(i) {
                    None
                } else {
                    geohash::decode(str_arr.value(i))
                        .ok()
                        .map(|(coord, _, _)| coord.y)
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn create_geo_hash_decode_lat_udf() -> ScalarUDF {
    ScalarUDF::from(GeoHashDecodeLatUDF::new())
}

// ---------------------------------------------------------------------------
// geo_hash_decode_lon(geohash) → Float64
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GeoHashDecodeLonUDF {
    signature: Signature,
}

impl GeoHashDecodeLonUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeoHashDecodeLonUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geo_hash_decode_lon"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(DataFusionError::Execution(
                "geo_hash_decode_lon requires 1 argument: geohash".to_string(),
            ));
        }

        // Scalar fast path
        if let ColumnarValue::Scalar(s) = &args[0] {
            if s.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
            }
            let hash = scalar_to_string(s)?;
            return match geohash::decode(&hash) {
                Ok((coord, _, _)) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(coord.x)))),
                Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(None))),
            };
        }

        // Array path
        let len = batch_length(&args);
        let arr = columnar_to_array(&args[0], len)?;
        let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Execution("geo_hash_decode_lon: argument must be Utf8".into())
        })?;

        let result: Float64Array = (0..len)
            .map(|i| {
                if str_arr.is_null(i) {
                    None
                } else {
                    geohash::decode(str_arr.value(i))
                        .ok()
                        .map(|(coord, _, _)| coord.x)
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn create_geo_hash_decode_lon_udf() -> ScalarUDF {
    ScalarUDF::from(GeoHashDecodeLonUDF::new())
}

// ---------------------------------------------------------------------------
// geo_hash_neighbors(geohash) → Utf8 (comma-separated 8 neighbors)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GeoHashNeighborsUDF {
    signature: Signature,
}

impl GeoHashNeighborsUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

/// Format the 8 neighbors as a comma-separated string.
fn format_neighbors(n: &geohash::Neighbors) -> String {
    [&n.n, &n.ne, &n.e, &n.se, &n.s, &n.sw, &n.w, &n.nw]
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

impl ScalarUDFImpl for GeoHashNeighborsUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geo_hash_neighbors"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(DataFusionError::Execution(
                "geo_hash_neighbors requires 1 argument: geohash".to_string(),
            ));
        }

        // Scalar fast path
        if let ColumnarValue::Scalar(s) = &args[0] {
            if s.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
            }
            let hash = scalar_to_string(s)?;
            return match geohash::neighbors(&hash) {
                Ok(n) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    format_neighbors(&n),
                )))),
                Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            };
        }

        // Array path
        let len = batch_length(&args);
        let arr = columnar_to_array(&args[0], len)?;
        let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Execution("geo_hash_neighbors: argument must be Utf8".into())
        })?;

        let result: StringArray = (0..len)
            .map(|i| {
                if str_arr.is_null(i) {
                    None
                } else {
                    geohash::neighbors(str_arr.value(i))
                        .ok()
                        .map(|n| format_neighbors(&n))
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn create_geo_hash_neighbors_udf() -> ScalarUDF {
    ScalarUDF::from(GeoHashNeighborsUDF::new())
}

// ---------------------------------------------------------------------------
// geo_within(lat, lon, center_lat, center_lon, radius_m) → Boolean
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GeoWithinUDF {
    signature: Signature,
}

impl GeoWithinUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeoWithinUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geo_within"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 5 {
            return Err(DataFusionError::Execution(
                "geo_within requires 5 arguments: lat, lon, center_lat, center_lon, radius_m"
                    .to_string(),
            ));
        }

        // All-scalar fast path
        if let (
            ColumnarValue::Scalar(s0),
            ColumnarValue::Scalar(s1),
            ColumnarValue::Scalar(s2),
            ColumnarValue::Scalar(s3),
            ColumnarValue::Scalar(s4),
        ) = (&args[0], &args[1], &args[2], &args[3], &args[4])
        {
            if s0.is_null() || s1.is_null() || s2.is_null() || s3.is_null() || s4.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
            }
            let (lat, lon, clat, clon, radius) = (
                scalar_to_f64(s0)?,
                scalar_to_f64(s1)?,
                scalar_to_f64(s2)?,
                scalar_to_f64(s3)?,
                scalar_to_f64(s4)?,
            );
            let d = Point::new(lon, lat).haversine_distance(&Point::new(clon, clat));
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                d <= radius,
            ))));
        }

        // Array path
        let len = batch_length(&args);
        let arrs: Vec<ArrayRef> = args.iter().map(|a| columnar_to_array(a, len)).collect::<Result<_>>()?;
        let lat = arrs[0].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_within: lat must be Float64".into())
        })?;
        let lon = arrs[1].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_within: lon must be Float64".into())
        })?;
        let clat = arrs[2].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_within: center_lat must be Float64".into())
        })?;
        let clon = arrs[3].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_within: center_lon must be Float64".into())
        })?;
        let radius = arrs[4].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_within: radius_m must be Float64".into())
        })?;

        let result: BooleanArray = (0..len)
            .map(|i| {
                if lat.is_null(i)
                    || lon.is_null(i)
                    || clat.is_null(i)
                    || clon.is_null(i)
                    || radius.is_null(i)
                {
                    None
                } else {
                    let d = Point::new(lon.value(i), lat.value(i))
                        .haversine_distance(&Point::new(clon.value(i), clat.value(i)));
                    Some(d <= radius.value(i))
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn create_geo_within_udf() -> ScalarUDF {
    ScalarUDF::from(GeoWithinUDF::new())
}

// ---------------------------------------------------------------------------
// geo_bounding_box(lat, lon, min_lat, min_lon, max_lat, max_lon) → Boolean
//
// Note: Does not handle antimeridian (180°/-180°) crossing.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GeoBoundingBoxUDF {
    signature: Signature,
}

impl GeoBoundingBoxUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeoBoundingBoxUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geo_bounding_box"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 6 {
            return Err(DataFusionError::Execution(
                "geo_bounding_box requires 6 arguments: lat, lon, min_lat, min_lon, max_lat, max_lon"
                    .to_string(),
            ));
        }

        // All-scalar fast path
        if let (
            ColumnarValue::Scalar(s0),
            ColumnarValue::Scalar(s1),
            ColumnarValue::Scalar(s2),
            ColumnarValue::Scalar(s3),
            ColumnarValue::Scalar(s4),
            ColumnarValue::Scalar(s5),
        ) = (&args[0], &args[1], &args[2], &args[3], &args[4], &args[5])
        {
            if s0.is_null()
                || s1.is_null()
                || s2.is_null()
                || s3.is_null()
                || s4.is_null()
                || s5.is_null()
            {
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
            }
            let (lat, lon, min_lat, min_lon, max_lat, max_lon) = (
                scalar_to_f64(s0)?,
                scalar_to_f64(s1)?,
                scalar_to_f64(s2)?,
                scalar_to_f64(s3)?,
                scalar_to_f64(s4)?,
                scalar_to_f64(s5)?,
            );
            let inside =
                lat >= min_lat && lat <= max_lat && lon >= min_lon && lon <= max_lon;
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(inside))));
        }

        // Array path
        let len = batch_length(&args);
        let arrs: Vec<ArrayRef> = args.iter().map(|a| columnar_to_array(a, len)).collect::<Result<_>>()?;
        let lat = arrs[0].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bounding_box: lat must be Float64".into())
        })?;
        let lon = arrs[1].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bounding_box: lon must be Float64".into())
        })?;
        let min_lat = arrs[2].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bounding_box: min_lat must be Float64".into())
        })?;
        let min_lon = arrs[3].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bounding_box: min_lon must be Float64".into())
        })?;
        let max_lat = arrs[4].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bounding_box: max_lat must be Float64".into())
        })?;
        let max_lon = arrs[5].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bounding_box: max_lon must be Float64".into())
        })?;

        let result: BooleanArray = (0..len)
            .map(|i| {
                if lat.is_null(i)
                    || lon.is_null(i)
                    || min_lat.is_null(i)
                    || min_lon.is_null(i)
                    || max_lat.is_null(i)
                    || max_lon.is_null(i)
                {
                    None
                } else {
                    let inside = lat.value(i) >= min_lat.value(i)
                        && lat.value(i) <= max_lat.value(i)
                        && lon.value(i) >= min_lon.value(i)
                        && lon.value(i) <= max_lon.value(i);
                    Some(inside)
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn create_geo_bounding_box_udf() -> ScalarUDF {
    ScalarUDF::from(GeoBoundingBoxUDF::new())
}

// ---------------------------------------------------------------------------
// geo_bearing(lat1, lon1, lat2, lon2) → Float64 degrees [0, 360)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GeoBearingUDF {
    signature: Signature,
}

impl GeoBearingUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

/// Normalize bearing to [0, 360) range.
#[inline]
fn normalize_bearing(bearing: f64) -> f64 {
    ((bearing % 360.0) + 360.0) % 360.0
}

impl ScalarUDFImpl for GeoBearingUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "geo_bearing"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 4 {
            return Err(DataFusionError::Execution(
                "geo_bearing requires 4 arguments: lat1, lon1, lat2, lon2".to_string(),
            ));
        }

        // All-scalar fast path
        if let (
            ColumnarValue::Scalar(s0),
            ColumnarValue::Scalar(s1),
            ColumnarValue::Scalar(s2),
            ColumnarValue::Scalar(s3),
        ) = (&args[0], &args[1], &args[2], &args[3])
        {
            if s0.is_null() || s1.is_null() || s2.is_null() || s3.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
            }
            let (lat1, lon1, lat2, lon2) = (
                scalar_to_f64(s0)?,
                scalar_to_f64(s1)?,
                scalar_to_f64(s2)?,
                scalar_to_f64(s3)?,
            );
            let b = Point::new(lon1, lat1).haversine_bearing(Point::new(lon2, lat2));
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
                normalize_bearing(b),
            ))));
        }

        // Array path
        let len = batch_length(&args);
        let arrs: Vec<ArrayRef> = args.iter().map(|a| columnar_to_array(a, len)).collect::<Result<_>>()?;
        let lat1 = arrs[0].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bearing: lat1 must be Float64".into())
        })?;
        let lon1 = arrs[1].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bearing: lon1 must be Float64".into())
        })?;
        let lat2 = arrs[2].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bearing: lat2 must be Float64".into())
        })?;
        let lon2 = arrs[3].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("geo_bearing: lon2 must be Float64".into())
        })?;

        let result: Float64Array = (0..len)
            .map(|i| {
                if lat1.is_null(i) || lon1.is_null(i) || lat2.is_null(i) || lon2.is_null(i) {
                    None
                } else {
                    let b = Point::new(lon1.value(i), lat1.value(i))
                        .haversine_bearing(Point::new(lon2.value(i), lat2.value(i)));
                    Some(normalize_bearing(b))
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn create_geo_bearing_udf() -> ScalarUDF {
    ScalarUDF::from(GeoBearingUDF::new())
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::common::ScalarValue;
    use datafusion::prelude::*;

    // -----------------------------------------------------------------------
    // geo_distance tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_geo_distance_scalar_nyc_london() {
        let udf = create_geo_distance_udf();
        // NYC (40.7128, -74.0060) → London (51.5074, -0.1278)
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(40.7128))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-74.0060))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(51.5074))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-0.1278))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(d))) => {
                // ~5,570 km
                assert!(d > 5_500_000.0 && d < 5_600_000.0, "distance was {d}");
            }
            _ => panic!("expected scalar Float64"),
        }
    }

    #[test]
    fn test_geo_distance_same_point() {
        let udf = create_geo_distance_udf();
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(40.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-74.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(40.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-74.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(d))) => {
                assert!((d - 0.0).abs() < 0.001, "distance was {d}");
            }
            _ => panic!("expected scalar Float64"),
        }
    }

    #[test]
    fn test_geo_distance_null_propagation() {
        let udf = create_geo_distance_udf();
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(None)),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-74.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(51.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-0.1))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {}
            _ => panic!("expected null scalar"),
        }
    }

    #[test]
    fn test_geo_distance_array() {
        let udf = create_geo_distance_udf();
        let lat1 = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(40.7128),
            None,
            Some(0.0),
        ])));
        let lon1 = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(-74.006),
            Some(0.0),
            Some(0.0),
        ])));
        let lat2 = ColumnarValue::Scalar(ScalarValue::Float64(Some(51.5074)));
        let lon2 = ColumnarValue::Scalar(ScalarValue::Float64(Some(-0.1278)));

        let result = udf.invoke_batch(&[lat1, lon1, lat2, lon2], 3).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(arr.len(), 3);
                assert!(!arr.is_null(0));
                assert!(arr.value(0) > 5_500_000.0);
                assert!(arr.is_null(1));
                assert!(!arr.is_null(2));
            }
            _ => panic!("expected array"),
        }
    }

    // -----------------------------------------------------------------------
    // geo_hash tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_geo_hash_known_coords() {
        let udf = create_geo_hash_udf();
        // San Francisco area (37.7749, -122.4194) at precision 5
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(37.7749))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.4194))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(h))) => {
                assert_eq!(h.len(), 5);
                // Verify it starts with "9q8y" (known geohash prefix for SF)
                assert!(h.starts_with("9q8y"), "got {h}");
            }
            _ => panic!("expected scalar Utf8"),
        }
    }

    #[test]
    fn test_geo_hash_precision_7() {
        let udf = create_geo_hash_udf();
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(37.7749))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.4194))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(h))) => {
                assert_eq!(h.len(), 7);
            }
            _ => panic!("expected scalar Utf8"),
        }
    }

    #[test]
    fn test_geo_hash_null_input() {
        let udf = create_geo_hash_udf();
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(None)),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.4194))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            _ => panic!("expected null scalar"),
        }
    }

    // -----------------------------------------------------------------------
    // geo_hash_decode roundtrip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_geo_hash_decode_roundtrip() {
        let hash_udf = create_geo_hash_udf();
        let decode_lat_udf = create_geo_hash_decode_lat_udf();
        let decode_lon_udf = create_geo_hash_decode_lon_udf();

        // Encode at precision 9 for high accuracy
        let lat = 37.7749;
        let lon = -122.4194;
        let hash_result = hash_udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(lat))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(lon))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(9))),
                ],
                1,
            )
            .unwrap();

        let hash = match hash_result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(h))) => h,
            _ => panic!("expected hash string"),
        };

        // Decode lat
        let lat_result = decode_lat_udf
            .invoke_batch(
                &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash.clone())))],
                1,
            )
            .unwrap();
        let decoded_lat = match lat_result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
            _ => panic!("expected Float64"),
        };

        // Decode lon
        let lon_result = decode_lon_udf
            .invoke_batch(
                &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash)))],
                1,
            )
            .unwrap();
        let decoded_lon = match lon_result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
            _ => panic!("expected Float64"),
        };

        // At precision 9, error should be < 0.001 degrees
        assert!(
            (decoded_lat - lat).abs() < 0.001,
            "lat: {decoded_lat} vs {lat}"
        );
        assert!(
            (decoded_lon - lon).abs() < 0.001,
            "lon: {decoded_lon} vs {lon}"
        );
    }

    #[test]
    fn test_geo_hash_decode_invalid_hash() {
        let udf = create_geo_hash_decode_lat_udf();
        let result = udf
            .invoke_batch(
                &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "!!!invalid!!!".to_string(),
                )))],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {}
            _ => panic!("expected null for invalid hash"),
        }
    }

    // -----------------------------------------------------------------------
    // geo_hash_neighbors tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_geo_hash_neighbors_known() {
        let udf = create_geo_hash_neighbors_udf();
        let result = udf
            .invoke_batch(
                &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "9q8yy".to_string(),
                )))],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let neighbors: Vec<&str> = s.split(',').collect();
                assert_eq!(neighbors.len(), 8, "expected 8 neighbors, got: {s}");
                // Each neighbor should be same length as input
                for n in &neighbors {
                    assert_eq!(n.len(), 5, "neighbor {n} wrong length");
                }
            }
            _ => panic!("expected scalar Utf8"),
        }
    }

    #[test]
    fn test_geo_hash_neighbors_null() {
        let udf = create_geo_hash_neighbors_udf();
        let result = udf
            .invoke_batch(
                &[ColumnarValue::Scalar(ScalarValue::Utf8(None))],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            _ => panic!("expected null"),
        }
    }

    // -----------------------------------------------------------------------
    // geo_within tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_geo_within_inside() {
        let udf = create_geo_within_udf();
        // Point very close to center (same coords, distance ~0)
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(37.7749))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.4194))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(37.7749))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.4194))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(1000.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {}
            _ => panic!("expected true"),
        }
    }

    #[test]
    fn test_geo_within_outside() {
        let udf = create_geo_within_udf();
        // NYC is ~5,570km from London — way outside 1km radius
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(40.7128))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-74.0060))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(51.5074))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-0.1278))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(1000.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))) => {}
            _ => panic!("expected false"),
        }
    }

    #[test]
    fn test_geo_within_array() {
        let udf = create_geo_within_udf();
        // Two points: one close (same point), one far
        let lat = ColumnarValue::Array(Arc::new(Float64Array::from(vec![37.7749, 40.7128])));
        let lon = ColumnarValue::Array(Arc::new(Float64Array::from(vec![-122.4194, -74.006])));
        let clat = ColumnarValue::Scalar(ScalarValue::Float64(Some(37.7749)));
        let clon = ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.4194)));
        let radius = ColumnarValue::Scalar(ScalarValue::Float64(Some(1000.0)));

        let result = udf
            .invoke_batch(&[lat, lon, clat, clon, radius], 2)
            .unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert!(arr.value(0)); // same point → inside
                assert!(!arr.value(1)); // NYC → outside
            }
            _ => panic!("expected array"),
        }
    }

    // -----------------------------------------------------------------------
    // geo_bounding_box tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_geo_bounding_box_inside() {
        let udf = create_geo_bounding_box_udf();
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(37.0))),  // lat
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.0))), // lon
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(36.0))),  // min_lat
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-123.0))), // min_lon
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(38.0))),  // max_lat
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-121.0))), // max_lon
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {}
            _ => panic!("expected true"),
        }
    }

    #[test]
    fn test_geo_bounding_box_outside() {
        let udf = create_geo_bounding_box_udf();
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(40.0))),  // lat outside
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(36.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-123.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(38.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-121.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))) => {}
            _ => panic!("expected false"),
        }
    }

    #[test]
    fn test_geo_bounding_box_edge() {
        let udf = create_geo_bounding_box_udf();
        // Point exactly on the boundary (min_lat)
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(36.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-122.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(36.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-123.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(38.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(-121.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {}
            _ => panic!("expected true for edge case"),
        }
    }

    #[test]
    fn test_geo_bounding_box_mixed_array() {
        let udf = create_geo_bounding_box_udf();
        let lat = ColumnarValue::Array(Arc::new(Float64Array::from(vec![37.0, 40.0, 36.0])));
        let lon = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            -122.0, -122.0, -122.0,
        ])));
        let min_lat = ColumnarValue::Scalar(ScalarValue::Float64(Some(36.0)));
        let min_lon = ColumnarValue::Scalar(ScalarValue::Float64(Some(-123.0)));
        let max_lat = ColumnarValue::Scalar(ScalarValue::Float64(Some(38.0)));
        let max_lon = ColumnarValue::Scalar(ScalarValue::Float64(Some(-121.0)));

        let result = udf
            .invoke_batch(&[lat, lon, min_lat, min_lon, max_lat, max_lon], 3)
            .unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert!(arr.value(0)); // inside
                assert!(!arr.value(1)); // outside (lat 40 > 38)
                assert!(arr.value(2)); // edge (lat == min_lat)
            }
            _ => panic!("expected array"),
        }
    }

    // -----------------------------------------------------------------------
    // geo_bearing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_geo_bearing_north() {
        let udf = create_geo_bearing_udf();
        // Going straight north: same longitude, increasing latitude
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(b))) => {
                assert!(b < 1.0 || b > 359.0, "north bearing was {b}");
            }
            _ => panic!("expected scalar Float64"),
        }
    }

    #[test]
    fn test_geo_bearing_east() {
        let udf = create_geo_bearing_udf();
        // Going straight east: same latitude, increasing longitude
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(b))) => {
                assert!((b - 90.0).abs() < 1.0, "east bearing was {b}");
            }
            _ => panic!("expected scalar Float64"),
        }
    }

    #[test]
    fn test_geo_bearing_always_positive() {
        let udf = create_geo_bearing_udf();
        // Going south-west: bearing should be ~225° (not negative)
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(b))) => {
                assert!(b >= 0.0 && b < 360.0, "bearing was {b}");
                assert!((b - 225.0).abs() < 5.0, "SW bearing was {b}");
            }
            _ => panic!("expected scalar Float64"),
        }
    }

    #[test]
    fn test_geo_bearing_null() {
        let udf = create_geo_bearing_udf();
        let result = udf
            .invoke_batch(
                &[
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(None)),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                ],
                1,
            )
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {}
            _ => panic!("expected null"),
        }
    }

    // -----------------------------------------------------------------------
    // SQL integration test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_geo_udfs_in_sql() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_geo_distance_udf());
        ctx.register_udf(create_geo_hash_udf());
        ctx.register_udf(create_geo_hash_decode_lat_udf());
        ctx.register_udf(create_geo_hash_decode_lon_udf());
        ctx.register_udf(create_geo_hash_neighbors_udf());
        ctx.register_udf(create_geo_within_udf());
        ctx.register_udf(create_geo_bounding_box_udf());
        ctx.register_udf(create_geo_bearing_udf());

        // Create a simple in-memory table
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", DataType::Int64, false),
            datafusion::arrow::datatypes::Field::new("lat", DataType::Float64, false),
            datafusion::arrow::datatypes::Field::new("lon", DataType::Float64, false),
        ]));

        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![40.7128, 51.5074, 37.7749])),
                Arc::new(Float64Array::from(vec![-74.006, -0.1278, -122.4194])),
            ],
        )
        .unwrap();

        ctx.register_batch("locations", batch).unwrap();

        // Test geo_distance in SQL
        let df = ctx
            .sql("SELECT id, geo_distance(lat, lon, 40.7128, -74.006) as dist FROM locations ORDER BY id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let dist_arr = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // First row is same point → distance ~0
        assert!(dist_arr.value(0) < 1.0);
        // Second row NYC→London ~5,570km
        assert!(dist_arr.value(1) > 5_500_000.0);

        // Test geo_within in SQL
        let df = ctx
            .sql("SELECT id FROM locations WHERE geo_within(lat, lon, 40.7128, -74.006, 100000.0) ORDER BY id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let id_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Only NYC (id=1) should be within 100km of NYC
        assert_eq!(id_arr.len(), 1);
        assert_eq!(id_arr.value(0), 1);

        // Test geo_hash in SQL
        let df = ctx
            .sql("SELECT geo_hash(lat, lon, 5) as hash FROM locations ORDER BY id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let hash_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(hash_arr.value(0).len(), 5);
        assert_eq!(hash_arr.value(1).len(), 5);
        assert_eq!(hash_arr.value(2).len(), 5);

        // Test geo_bearing in SQL
        let df = ctx
            .sql("SELECT geo_bearing(0.0, 0.0, 1.0, 0.0) as bearing")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let bearing_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Bearing north should be ~0
        assert!(bearing_arr.value(0) < 1.0 || bearing_arr.value(0) > 359.0);
    }
}
