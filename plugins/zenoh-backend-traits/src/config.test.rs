//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use std::time::Duration;

use serde_json::json;

use super::StorageConfig;
use crate::config::ReplicaConfig;

#[test]
fn test_replica_config() {
    let empty_config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {}
    });
    let storage_config =
        StorageConfig::try_from("test-plugin", "test-storage", &empty_config).unwrap();
    assert_eq!(storage_config.replication, Some(ReplicaConfig::default()));

    let incorrect_propagation_delay_config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "interval": 1,
            "propagation_delay": 750,
        }
    });
    let result = StorageConfig::try_from(
        "test-plugin",
        "test-storage",
        &incorrect_propagation_delay_config,
    );
    let err = result.unwrap_err();
    let expected_error_msg =
        "consider increasing the `interval` to at least twice its value (i.e. 1.5)";
    assert!(
        err.to_string().contains(expected_error_msg),
        "\nExpected to contain: {expected_error_msg}
Actual message: {err}",
    );

    let replica_config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "interval": 10,
            "sub_intervals": 4,
            "hot": 6,
            "warm": 60,
            "propagation_delay": 250,
        }
    });
    let storage_config =
        StorageConfig::try_from("test-plugin", "test-storage", &replica_config).unwrap();
    assert_eq!(
        storage_config.replication,
        Some(ReplicaConfig {
            interval: Duration::from_secs(10),
            sub_intervals: 4,
            hot: 6,
            warm: 60,
            propagation_delay: Duration::from_millis(250),
            batch_size: 100,
            bloom_filter_capacity: None,
            bloom_filter_fp_rate: None,
        })
    );
}

// --- Piece 1: batch_size config tests ---

#[test]
fn piece1_batch_size_defaults_to_100() {
    let config = ReplicaConfig::default();
    assert_eq!(
        config.batch_size, 100,
        "ReplicaConfig::default().batch_size should be 100"
    );
}

#[test]
fn piece1_batch_size_parseable_from_json_config() {
    let config_with_batch_size = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "batch_size": 50,
        }
    });
    let storage_config =
        StorageConfig::try_from("test-plugin", "test-storage", &config_with_batch_size).unwrap();
    let replica = storage_config.replication.expect("replication config should be Some");
    assert_eq!(
        replica.batch_size, 50,
        "batch_size should be parsed from JSON config"
    );
}

#[test]
fn piece1_batch_size_uses_default_when_omitted_in_json() {
    let config_without_batch_size = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {}
    });
    let storage_config =
        StorageConfig::try_from("test-plugin", "test-storage", &config_without_batch_size).unwrap();
    let replica = storage_config.replication.expect("replication config should be Some");
    assert_eq!(
        replica.batch_size, 100,
        "batch_size should default to 100 when not specified in JSON"
    );
}

#[test]
fn piece1_batch_size_zero_is_rejected() {
    let config_with_zero_batch_size = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "batch_size": 0,
        }
    });
    let result = StorageConfig::try_from(
        "test-plugin",
        "test-storage",
        &config_with_zero_batch_size,
    );
    assert!(
        result.is_err(),
        "batch_size of 0 should be rejected"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("must be greater than 0"),
        "Error should mention the constraint: {}",
        err
    );
}

// --- bloom filter config tests ---

#[test]
fn bloom_filter_fields_default_to_none() {
    let config = ReplicaConfig::default();
    assert_eq!(config.bloom_filter_capacity, None);
    assert_eq!(config.bloom_filter_fp_rate, None);
}

#[test]
fn bloom_filter_capacity_parseable_from_json() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_capacity": 1000000,
        }
    });
    let storage = StorageConfig::try_from("test-plugin", "test-storage", &config).unwrap();
    let replica = storage.replication.unwrap();
    assert_eq!(replica.bloom_filter_capacity, Some(1_000_000));
}

#[test]
fn bloom_filter_fp_rate_parseable_from_json() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_fp_rate": 0.001,
        }
    });
    let storage = StorageConfig::try_from("test-plugin", "test-storage", &config).unwrap();
    let replica = storage.replication.unwrap();
    assert_eq!(replica.bloom_filter_fp_rate, Some(0.001));
}

#[test]
fn bloom_filter_capacity_zero_rejected() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_capacity": 0,
        }
    });
    assert!(StorageConfig::try_from("test-plugin", "test-storage", &config).is_err());
}

#[test]
fn bloom_filter_fp_rate_zero_rejected() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_fp_rate": 0.0,
        }
    });
    assert!(StorageConfig::try_from("test-plugin", "test-storage", &config).is_err());
}

#[test]
fn bloom_filter_fp_rate_one_rejected() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_fp_rate": 1.0,
        }
    });
    assert!(StorageConfig::try_from("test-plugin", "test-storage", &config).is_err());
}

#[test]
fn bloom_filter_fp_rate_above_one_rejected() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_fp_rate": 1.5,
        }
    });
    assert!(StorageConfig::try_from("test-plugin", "test-storage", &config).is_err());
}

#[test]
fn bloom_filter_fields_absent_means_none() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {}
    });
    let storage = StorageConfig::try_from("test-plugin", "test-storage", &config).unwrap();
    let replica = storage.replication.unwrap();
    assert_eq!(replica.bloom_filter_capacity, None);
    assert_eq!(replica.bloom_filter_fp_rate, None);
}
