//! Tag cardinality tracking for optimal column ordering

use std::collections::{HashMap, HashSet};

/// Maximum distinct values to track per tag before marking as "high cardinality"
/// This bounds memory usage while still allowing cardinality-based sorting
pub const MAX_TRACKED_DISTINCT_VALUES: usize = 1000;

/// Tag cardinality tracker for a single table
/// Uses bounded HashSets to efficiently track distinct values during writes
#[derive(Default)]
pub struct TableCardinalityTracker {
    /// Maps tag_key -> set of distinct values (bounded)
    tag_values: HashMap<String, HashSet<String>>,
    /// Tags that exceeded MAX_TRACKED_DISTINCT_VALUES
    high_cardinality_tags: HashSet<String>,
}

impl TableCardinalityTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Track tag values from a point, respecting the memory bound
    pub fn track_point(&mut self, tags: &HashMap<String, String>) {
        for (key, value) in tags {
            // Skip if already marked as high cardinality
            if self.high_cardinality_tags.contains(key) {
                continue;
            }

            let values = self.tag_values.entry(key.clone()).or_default();

            // Check if we've hit the threshold
            if values.len() >= MAX_TRACKED_DISTINCT_VALUES {
                // Mark as high cardinality and clear the set to free memory
                self.high_cardinality_tags.insert(key.clone());
                self.tag_values.remove(key);
            } else {
                values.insert(value.clone());
            }
        }
    }

    /// Get cardinality estimate for a tag key
    /// Returns usize::MAX for high cardinality tags
    pub fn get_cardinality(&self, key: &str) -> usize {
        if self.high_cardinality_tags.contains(key) {
            usize::MAX
        } else {
            self.tag_values.get(key).map(|s| s.len()).unwrap_or(0)
        }
    }

    /// Get all tag keys sorted by cardinality (low to high)
    pub fn get_tags_by_cardinality(&self) -> Vec<String> {
        let mut all_tags: HashSet<String> = self.tag_values.keys().cloned().collect();
        all_tags.extend(self.high_cardinality_tags.iter().cloned());

        let mut tags_with_cardinality: Vec<(String, usize)> = all_tags
            .into_iter()
            .map(|key| {
                let card = self.get_cardinality(&key);
                (key, card)
            })
            .collect();

        // Sort by cardinality (ascending - low cardinality first)
        tags_with_cardinality.sort_by_key(|(_, card)| *card);

        tags_with_cardinality.into_iter().map(|(k, _)| k).collect()
    }

    /// Clear the tracker (called after flush)
    pub fn clear(&mut self) {
        self.tag_values.clear();
        self.high_cardinality_tags.clear();
    }
}
