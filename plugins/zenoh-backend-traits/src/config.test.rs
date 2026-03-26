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

use std::str::FromStr;
use std::time::Duration;

use serde_json::json;
use zenoh::key_expr::OwnedKeyExpr;

use super::StorageConfig;
use crate::config::{GarbageCollectionConfig, PrefixLifespan, ReplicaConfig};

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
            bloom_filter_fp_rate_permille: None,
        })
    );
}

#[test]
fn test_prefix_lifespan_config() {
    let config = json!({
        "key_expr": "devices/**",
        "volume": "memory",
        "garbage_collection": {
            "period": 60,
            "lifespan": 86400,
            "prefix_lifespans": [
                {
                    "key_expr": "**/events/**",
                    "lifespan": 172800,
                    "delete_data": true
                },
                {
                    "key_expr": "telemetry/**",
                    "lifespan": 3600,
                    "delete_data": false
                }
            ]
        }
    });

    let storage_config =
        StorageConfig::try_from("test-plugin", "test-storage", &config).unwrap();
    let gc = &storage_config.garbage_collection_config;

    assert_eq!(gc.period, Duration::from_secs(60));
    assert_eq!(gc.lifespan, Duration::from_secs(86400));

    let prefix_lifespans = gc.prefix_lifespans.as_ref().expect("prefix_lifespans should be Some");
    assert_eq!(prefix_lifespans.len(), 2);

    assert_eq!(
        prefix_lifespans[0],
        PrefixLifespan {
            key_expr: OwnedKeyExpr::from_str("**/events/**").unwrap(),
            lifespan: Duration::from_secs(172800),
            delete_data: true,
        }
    );
    assert_eq!(
        prefix_lifespans[1],
        PrefixLifespan {
            key_expr: OwnedKeyExpr::from_str("telemetry/**").unwrap(),
            lifespan: Duration::from_secs(3600),
            delete_data: false,
        }
    );
}

#[test]
fn test_prefix_lifespan_backward_compat() {
    // Missing prefix_lifespans field should default to None
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "garbage_collection": {
            "period": 30,
            "lifespan": 86400
        }
    });

    let storage_config =
        StorageConfig::try_from("test-plugin", "test-storage", &config).unwrap();
    assert_eq!(
        storage_config.garbage_collection_config,
        GarbageCollectionConfig::default()
    );
    assert!(storage_config.garbage_collection_config.prefix_lifespans.is_none());
}

#[test]
fn test_prefix_lifespan_no_gc_section() {
    // No garbage_collection section at all should use defaults
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory"
    });

    let storage_config =
        StorageConfig::try_from("test-plugin", "test-storage", &config).unwrap();
    assert_eq!(
        storage_config.garbage_collection_config,
        GarbageCollectionConfig::default()
    );
    assert!(storage_config.garbage_collection_config.prefix_lifespans.is_none());
}

#[test]
fn test_prefix_lifespan_invalid_lifespan() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "garbage_collection": {
            "prefix_lifespans": [
                {
                    "key_expr": "**/events/**",
                    "lifespan": "not_a_number",
                    "delete_data": true
                }
            ]
        }
    });

    let result = StorageConfig::try_from("test-plugin", "test-storage", &config);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("lifespan"),
        "Error should mention lifespan: {err}"
    );
}

#[test]
fn test_prefix_lifespan_missing_key_expr() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "garbage_collection": {
            "prefix_lifespans": [
                {
                    "lifespan": 3600,
                    "delete_data": true
                }
            ]
        }
    });

    let result = StorageConfig::try_from("test-plugin", "test-storage", &config);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("key_expr"),
        "Error should mention key_expr: {err}"
    );
}

#[test]
fn test_prefix_lifespan_invalid_type_not_array() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "garbage_collection": {
            "prefix_lifespans": "not_an_array"
        }
    });

    let result = StorageConfig::try_from("test-plugin", "test-storage", &config);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("prefix_lifespans"),
        "Error should mention prefix_lifespans: {err}"
    );
}

#[test]
fn test_prefix_lifespan_missing_lifespan_field() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "garbage_collection": {
            "prefix_lifespans": [
                {
                    "key_expr": "**/events/**",
                    "delete_data": true
                }
            ]
        }
    });

    let result = StorageConfig::try_from("test-plugin", "test-storage", &config);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("lifespan"),
        "Error should mention lifespan: {err}"
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
    assert_eq!(config.bloom_filter_fp_rate_permille, None);
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
fn bloom_filter_fp_rate_permille_parseable_from_json() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_fp_rate_permille": 5,
        }
    });
    let storage = StorageConfig::try_from("test-plugin", "test-storage", &config).unwrap();
    let replica = storage.replication.unwrap();
    assert_eq!(replica.bloom_filter_fp_rate_permille, Some(5));
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
fn bloom_filter_fp_rate_permille_zero_rejected() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_fp_rate_permille": 0,
        }
    });
    assert!(StorageConfig::try_from("test-plugin", "test-storage", &config).is_err());
}

#[test]
fn bloom_filter_fp_rate_permille_1000_rejected() {
    let config = json!({
        "key_expr": "test/**",
        "volume": "memory",
        "replication": {
            "bloom_filter_fp_rate_permille": 1000,
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
    assert_eq!(replica.bloom_filter_fp_rate_permille, None);
}
