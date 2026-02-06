//! Prometheus Remote Storage Protocol Types
//!
//! These are the protobuf message definitions for Prometheus remote read/write.
//! Based on: https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto

use prost::Message;

/// A label is a key-value pair
#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A sample is a timestamp/value pair
#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    /// Value of the sample
    #[prost(double, tag = "1")]
    pub value: f64,
    /// Timestamp in milliseconds since epoch
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// TimeSeries represents samples and labels for a single time series
#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    /// Labels identifying this time series (includes __name__ for metric name)
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    /// Samples (timestamp/value pairs)
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

/// WriteRequest is the request body for remote write
#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// LabelMatcher specifies a matcher for labels
#[derive(Clone, PartialEq, Message)]
pub struct LabelMatcher {
    /// Type of match (0=EQ, 1=NEQ, 2=RE, 3=NRE)
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    /// Label name
    #[prost(string, tag = "2")]
    pub name: String,
    /// Label value (for EQ/NEQ) or regex pattern (for RE/NRE)
    #[prost(string, tag = "3")]
    pub value: String,
}

/// MatchType specifies how to match labels
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(i32)]
pub enum MatchType {
    /// Equals
    #[default]
    Eq = 0,
    /// Not equals
    Neq = 1,
    /// Regex match
    Re = 2,
    /// Negative regex match
    Nre = 3,
}

impl MatchType {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(MatchType::Eq),
            1 => Some(MatchType::Neq),
            2 => Some(MatchType::Re),
            3 => Some(MatchType::Nre),
            _ => None,
        }
    }
}

/// Query is a single query within a ReadRequest
#[derive(Clone, PartialEq, Message)]
pub struct Query {
    /// Start timestamp in milliseconds
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    /// End timestamp in milliseconds
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    /// Label matchers for filtering
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
}

/// ReadRequest is the request body for remote read
#[derive(Clone, PartialEq, Message)]
pub struct ReadRequest {
    #[prost(message, repeated, tag = "1")]
    pub queries: Vec<Query>,
}

/// QueryResult contains the time series matching a single query
#[derive(Clone, PartialEq, Message)]
pub struct QueryResult {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// ReadResponse is the response body for remote read
#[derive(Clone, PartialEq, Message)]
pub struct ReadResponse {
    /// One result per query in the request
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<QueryResult>,
}

/// Constants for Prometheus label names
pub const METRIC_NAME_LABEL: &str = "__name__";

impl Label {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

impl Sample {
    pub fn new(timestamp_ms: i64, value: f64) -> Self {
        Self {
            value,
            timestamp: timestamp_ms,
        }
    }
}

impl TimeSeries {
    /// Get the metric name from labels
    pub fn metric_name(&self) -> Option<&str> {
        self.labels
            .iter()
            .find(|l| l.name == METRIC_NAME_LABEL)
            .map(|l| l.value.as_str())
    }

    /// Get labels excluding __name__
    pub fn tags(&self) -> impl Iterator<Item = &Label> {
        self.labels.iter().filter(|l| l.name != METRIC_NAME_LABEL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_request_encode_decode() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label::new(METRIC_NAME_LABEL, "cpu_usage"),
                    Label::new("host", "server01"),
                    Label::new("region", "us-west"),
                ],
                samples: vec![
                    Sample::new(1704067200000, 42.5),
                    Sample::new(1704067260000, 43.1),
                ],
            }],
        };

        // Encode
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        // Decode
        let decoded = WriteRequest::decode(&buf[..]).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn test_timeseries_helpers() {
        let ts = TimeSeries {
            labels: vec![
                Label::new(METRIC_NAME_LABEL, "http_requests_total"),
                Label::new("method", "GET"),
                Label::new("status", "200"),
            ],
            samples: vec![Sample::new(1704067200000, 100.0)],
        };

        assert_eq!(ts.metric_name(), Some("http_requests_total"));
        let tags: Vec<_> = ts.tags().collect();
        assert_eq!(tags.len(), 2);
        assert!(tags.iter().all(|l| l.name != METRIC_NAME_LABEL));
    }

    #[test]
    fn test_read_request_encode_decode() {
        let req = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 1704067200000,
                end_timestamp_ms: 1704153600000,
                matchers: vec![
                    LabelMatcher {
                        r#type: MatchType::Eq as i32,
                        name: METRIC_NAME_LABEL.to_string(),
                        value: "cpu_usage".to_string(),
                    },
                    LabelMatcher {
                        r#type: MatchType::Re as i32,
                        name: "host".to_string(),
                        value: "server.*".to_string(),
                    },
                ],
            }],
        };

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        let decoded = ReadRequest::decode(&buf[..]).unwrap();
        assert_eq!(req, decoded);
    }
}
