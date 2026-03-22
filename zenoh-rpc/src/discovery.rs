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

/// FNV-1a hash for producing compact, deterministic key expression hashes.
pub fn fnv1a_hash(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Build the liveliness key for a specific service instance.
///
/// Format: `@rpc/{key_expr_hash:x}/{zenoh_id}`
pub fn service_liveliness_token_key(key_expr: &str, zenoh_id: &str) -> String {
    let hash = fnv1a_hash(key_expr.as_bytes());
    format!("@rpc/{hash:x}/{zenoh_id}")
}

/// Build the liveliness query prefix to discover all instances of a service.
///
/// Format: `@rpc/{key_expr_hash:x}/**`
pub fn service_liveliness_prefix(key_expr: &str) -> String {
    let hash = fnv1a_hash(key_expr.as_bytes());
    format!("@rpc/{hash:x}/**")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_hash_is_deterministic() {
        let a = fnv1a_hash(b"test/service/key");
        let b = fnv1a_hash(b"test/service/key");
        assert_eq!(a, b);
    }

    #[test]
    fn fnv1a_hash_differs_for_different_inputs() {
        let a = fnv1a_hash(b"service/a");
        let b = fnv1a_hash(b"service/b");
        assert_ne!(a, b);
    }

    #[test]
    fn service_liveliness_token_key_format() {
        let key = service_liveliness_token_key("my/service", "abc123");
        assert!(key.starts_with("@rpc/"));
        assert!(key.ends_with("/abc123"));
        // Should contain the hex hash between the prefix and zenoh_id
        let parts: Vec<&str> = key.split('/').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "@rpc");
        assert_eq!(parts[2], "abc123");
    }

    #[test]
    fn service_liveliness_prefix_format() {
        let prefix = service_liveliness_prefix("my/service");
        assert!(prefix.starts_with("@rpc/"));
        assert!(prefix.ends_with("/**"));
    }

    #[test]
    fn token_key_and_prefix_share_hash() {
        let key = service_liveliness_token_key("svc/foo", "zid1");
        let prefix = service_liveliness_prefix("svc/foo");
        // The hash portion should match
        let key_hash = key.split('/').nth(1).unwrap();
        let prefix_hash = prefix.split('/').nth(1).unwrap();
        assert_eq!(key_hash, prefix_hash);
    }
}
