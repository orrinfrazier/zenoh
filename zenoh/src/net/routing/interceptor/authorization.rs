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

//! ⚠️ WARNING ⚠️
//!
//! This module is intended for Zenoh's internal use.
//!
//! [Click here for Zenoh's documentation](https://docs.rs/zenoh/latest/zenoh)
use std::collections::{HashMap, HashSet};

use ahash::RandomState;
use itertools::Itertools;
use zenoh_config::{
    AclConfig, AclConfigPolicyEntry, AclConfigRule, AclConfigSubjects, AclMessage, CertCommonName,
    InterceptorFlow, InterceptorLink, Interface, Permission, PolicyRule, Username, ZenohId,
};
use zenoh_keyexpr::{
    keyexpr,
    keyexpr_tree::{IKeyExprTree, IKeyExprTreeMut, IKeyExprTreeNode, KeBoxTree},
    OwnedNonWildKeyExpr,
};
use zenoh_result::ZResult;

use super::InterfaceEnabled;
type PolicyForSubject = FlowPolicy;

type PolicyMap = HashMap<usize, PolicyForSubject, RandomState>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Subject {
    pub(crate) interface: SubjectProperty<Interface>,
    pub(crate) cert_common_name: SubjectProperty<CertCommonName>,
    pub(crate) username: SubjectProperty<Username>,
    pub(crate) link_type: SubjectProperty<InterceptorLink>,
    pub(crate) zid: SubjectProperty<ZenohId>,
}

impl Subject {
    fn matches(&self, query: &SubjectQuery) -> bool {
        self.interface.matches(query.interface.as_ref())
            && self.username.matches(query.username.as_ref())
            && self
                .cert_common_name
                .matches(query.cert_common_name.as_ref())
            && self.link_type.matches(query.link_protocol.as_ref())
            && self.zid.matches(query.zid.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SubjectProperty<T> {
    Wildcard,
    Exactly(T),
}

impl<T: PartialEq + Eq> SubjectProperty<T> {
    fn matches(&self, other: Option<&T>) -> bool {
        match (self, other) {
            (SubjectProperty::Wildcard, None) => true,
            // NOTE: This match arm is the reason why `SubjectProperty` cannot simply be `Option`
            (SubjectProperty::Wildcard, Some(_)) => true,
            (SubjectProperty::Exactly(_), None) => false,
            (SubjectProperty::Exactly(lhs), Some(rhs)) => lhs == rhs,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SubjectQuery {
    pub(crate) interface: Option<Interface>,
    pub(crate) cert_common_name: Option<CertCommonName>,
    pub(crate) username: Option<Username>,
    pub(crate) link_protocol: Option<InterceptorLink>,
    pub(crate) zid: Option<ZenohId>,
}

impl std::fmt::Display for SubjectQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let subject_names = [
            self.interface.as_ref().map(|face| format!("{face}")),
            self.cert_common_name.as_ref().map(|ccn| format!("{ccn}")),
            self.username.as_ref().map(|username| format!("{username}")),
            self.link_protocol.as_ref().map(|link| format!("{link}")),
            self.zid.as_ref().map(|zid| format!("{zid}")),
        ];
        write!(
            f,
            "{}",
            subject_names
                .iter()
                .filter_map(|v| v.as_ref())
                .cloned()
                .collect::<Vec<_>>()
                .join("+")
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SubjectEntry {
    pub(crate) subject: Subject,
    pub(crate) id: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct SubjectStore {
    inner: Vec<SubjectEntry>,
}

impl SubjectStore {
    pub(crate) fn query<'a: 'b, 'b>(
        &'a self,
        query: &'b SubjectQuery,
    ) -> impl Iterator<Item = &'a SubjectEntry> + 'b {
        // FIXME: Can this search be better than linear?
        self.inner
            .iter()
            .filter(|entry| entry.subject.matches(query))
    }
}

impl Default for SubjectStore {
    fn default() -> Self {
        SubjectMapBuilder::new().build()
    }
}

pub(crate) struct SubjectMapBuilder {
    builder: HashMap<Subject, usize>,
    id_counter: usize,
}

impl SubjectMapBuilder {
    pub(crate) fn new() -> Self {
        Self {
            // FIXME: Capacity can be calculated from the length of subject properties in configuration
            builder: HashMap::new(),
            id_counter: 0,
        }
    }

    pub(crate) fn build(self) -> SubjectStore {
        SubjectStore {
            inner: self
                .builder
                .into_iter()
                .map(|(subject, id)| SubjectEntry { subject, id })
                .collect(),
        }
    }

    /// Assumes subject contains at most one instance of each Subject variant
    pub(crate) fn insert_or_get(&mut self, subject: Subject) -> usize {
        match self.builder.get(&subject).copied() {
            Some(id) => id,
            None => {
                self.id_counter += 1;
                self.builder.insert(subject, self.id_counter);
                self.id_counter
            }
        }
    }
}

type KeTreeRule = KeBoxTree<bool>;

#[derive(Default)]
struct PermissionPolicy {
    allow: KeTreeRule,
    deny: KeTreeRule,
}

impl PermissionPolicy {
    #[allow(dead_code)]
    fn permission(&self, permission: Permission) -> &KeTreeRule {
        match permission {
            Permission::Allow => &self.allow,
            Permission::Deny => &self.deny,
        }
    }
    fn permission_mut(&mut self, permission: Permission) -> &mut KeTreeRule {
        match permission {
            Permission::Allow => &mut self.allow,
            Permission::Deny => &mut self.deny,
        }
    }
}
#[derive(Default)]
struct ActionPolicy {
    query: PermissionPolicy,
    put: PermissionPolicy,
    delete: PermissionPolicy,
    declare_subscriber: PermissionPolicy,
    declare_queryable: PermissionPolicy,
    reply: PermissionPolicy,
    liveliness_token: PermissionPolicy,
    declare_liveliness_sub: PermissionPolicy,
    liveliness_query: PermissionPolicy,
}

impl ActionPolicy {
    fn action(&self, action: AclMessage) -> &PermissionPolicy {
        match action {
            AclMessage::Query => &self.query,
            AclMessage::Reply => &self.reply,
            AclMessage::Put => &self.put,
            AclMessage::Delete => &self.delete,
            AclMessage::DeclareSubscriber => &self.declare_subscriber,
            AclMessage::DeclareQueryable => &self.declare_queryable,
            AclMessage::LivelinessToken => &self.liveliness_token,
            AclMessage::DeclareLivelinessSubscriber => &self.declare_liveliness_sub,
            AclMessage::LivelinessQuery => &self.liveliness_query,
        }
    }
    fn action_mut(&mut self, action: AclMessage) -> &mut PermissionPolicy {
        match action {
            AclMessage::Query => &mut self.query,
            AclMessage::Reply => &mut self.reply,
            AclMessage::Put => &mut self.put,
            AclMessage::Delete => &mut self.delete,
            AclMessage::DeclareSubscriber => &mut self.declare_subscriber,
            AclMessage::DeclareQueryable => &mut self.declare_queryable,
            AclMessage::LivelinessToken => &mut self.liveliness_token,
            AclMessage::DeclareLivelinessSubscriber => &mut self.declare_liveliness_sub,
            AclMessage::LivelinessQuery => &mut self.liveliness_query,
        }
    }
}

#[derive(Default)]
pub struct FlowPolicy {
    ingress: ActionPolicy,
    egress: ActionPolicy,
}

impl FlowPolicy {
    fn flow(&self, flow: InterceptorFlow) -> &ActionPolicy {
        match flow {
            InterceptorFlow::Ingress => &self.ingress,
            InterceptorFlow::Egress => &self.egress,
        }
    }
    fn flow_mut(&mut self, flow: InterceptorFlow) -> &mut ActionPolicy {
        match flow {
            InterceptorFlow::Ingress => &mut self.ingress,
            InterceptorFlow::Egress => &mut self.egress,
        }
    }
}

pub struct PolicyEnforcer {
    pub(crate) acl_enabled: bool,
    pub(crate) default_permission: Permission,
    pub(crate) namespace: Option<OwnedNonWildKeyExpr>,
    pub(crate) subject_store: SubjectStore,
    pub(crate) policy_map: PolicyMap,
    pub(crate) interface_enabled: InterfaceEnabled,
}

#[derive(Debug, Clone)]
pub struct PolicyInformation {
    subject_map: SubjectStore,
    policy_rules: Vec<PolicyRule>,
}

impl PolicyEnforcer {
    pub fn new() -> PolicyEnforcer {
        PolicyEnforcer {
            acl_enabled: true,
            default_permission: Permission::Deny,
            namespace: None,
            subject_store: SubjectStore::default(),
            policy_map: PolicyMap::default(),
            interface_enabled: InterfaceEnabled::default(),
        }
    }

    /// Check if a key expression is under the configured namespace.
    /// Returns true if no namespace is set, or if the key is under the namespace.
    fn is_under_namespace(&self, key_expr: &keyexpr) -> bool {
        match &self.namespace {
            None => true,
            Some(ns) => {
                let ke_str = key_expr.as_str();
                let ns_str = ns.as_str();
                ke_str == ns_str
                    || (ke_str.starts_with(ns_str)
                        && ke_str.as_bytes().get(ns_str.len()) == Some(&b'/'))
            }
        }
    }

    /*
       initializes the policy_enforcer
    */
    pub fn init(&mut self, acl_config: &AclConfig) -> ZResult<()> {
        let mut_acl_config = acl_config.clone();
        self.acl_enabled = mut_acl_config.enabled;
        self.default_permission = mut_acl_config.default_permission;
        if self.acl_enabled {
            if let (Some(mut rules), Some(mut subjects), Some(policies)) = (
                mut_acl_config.rules,
                mut_acl_config.subjects,
                mut_acl_config.policies,
            ) {
                if rules.is_empty() || subjects.is_empty() || policies.is_empty() {
                    rules.is_empty().then(|| {
                        tracing::warn!("Access control rules list is empty in config file")
                    });
                    subjects.is_empty().then(|| {
                        tracing::warn!("Access control subjects list is empty in config file")
                    });
                    policies.is_empty().then(|| {
                        tracing::warn!("Access control policies list is empty in config file")
                    });
                    self.policy_map = PolicyMap::default();
                    self.subject_store = SubjectStore::default();
                    if self.default_permission == Permission::Deny || self.namespace.is_some() {
                        self.interface_enabled = InterfaceEnabled {
                            ingress: true,
                            egress: true,
                        };
                    }
                } else {
                    // check for undefined values in rules and initialize them to defaults
                    for rule in rules.iter_mut() {
                        if rule.id.trim().is_empty() {
                            bail!("Found empty rule id in rules list");
                        }
                        if rule.flows.is_none() {
                            tracing::warn!("Rule '{}' flows list is not set. Setting it to both Ingress and Egress", rule.id);
                            rule.flows = Some(
                                [InterceptorFlow::Ingress, InterceptorFlow::Egress]
                                    .to_vec()
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                    }
                    // check for undefined values in subjects and initialize them to defaults
                    for subject in subjects.iter_mut() {
                        if subject.id.trim().is_empty() {
                            bail!("Found empty subject id in subjects list");
                        }
                    }
                    let policy_information =
                        self.policy_information_point(subjects, rules, policies)?;

                    let mut main_policy: PolicyMap = PolicyMap::default();
                    for rule in policy_information.policy_rules {
                        let subject_policy = main_policy.entry(rule.subject_id).or_default();
                        subject_policy
                            .flow_mut(rule.flow)
                            .action_mut(rule.message)
                            .permission_mut(rule.permission)
                            .insert(&rule.key_expr, true);

                        if self.default_permission == Permission::Deny {
                            self.interface_enabled = InterfaceEnabled {
                                ingress: true,
                                egress: true,
                            };
                        } else {
                            match rule.flow {
                                InterceptorFlow::Ingress => {
                                    self.interface_enabled.ingress = true;
                                }
                                InterceptorFlow::Egress => {
                                    self.interface_enabled.egress = true;
                                }
                            }
                        }
                    }
                    self.policy_map = main_policy;
                    self.subject_store = policy_information.subject_map;
                    if self.namespace.is_some() {
                        self.interface_enabled = InterfaceEnabled {
                            ingress: true,
                            egress: true,
                        };
                    }
                }
            } else {
                bail!("All ACL rules/subjects/policies config lists must be provided");
            }
            if let Some(ns) = &self.namespace {
                tracing::info!(
                    "ACL auto-deny enabled for namespace '{}': keys outside '{}/{}' will be denied",
                    ns.as_str(),
                    ns.as_str(),
                    "**"
                );
            }
        }
        Ok(())
    }

    /*
       converts the sets of rules from config format into individual rules for each subject, key-expr, action, permission
    */
    pub fn policy_information_point(
        &self,
        subjects: Vec<AclConfigSubjects>,
        rules: Vec<AclConfigRule>,
        policies: Vec<AclConfigPolicyEntry>,
    ) -> ZResult<PolicyInformation> {
        let mut policy_rules: Vec<PolicyRule> = Vec::new();
        let mut rule_map = HashMap::new();
        let mut subject_id_map = HashMap::<String, Vec<usize>>::new();
        let mut policy_id_set = HashSet::<String>::new();
        let mut subject_map_builder = SubjectMapBuilder::new();

        // validate rules config and insert them in hashmaps
        for config_rule in rules {
            if rule_map.contains_key(&config_rule.id) {
                bail!(
                    "Rule id must be unique: id '{}' is repeated",
                    config_rule.id
                );
            }

            rule_map.insert(config_rule.id.clone(), config_rule);
        }

        for config_subject in subjects.into_iter() {
            if subject_id_map.contains_key(&config_subject.id) {
                bail!(
                    "Subject id must be unique: id '{}' is repeated",
                    config_subject.id
                );
            }
            // validate subject config fields
            if config_subject
                .interfaces
                .as_ref()
                .is_some_and(|interfaces| interfaces.iter().any(|face| face.0.trim().is_empty()))
            {
                bail!(
                    "Found empty interface value in subject '{}'",
                    config_subject.id
                );
            }
            if config_subject
                .cert_common_names
                .as_ref()
                .is_some_and(|cert_common_names| {
                    cert_common_names.iter().any(|ccn| ccn.0.trim().is_empty())
                })
            {
                bail!(
                    "Found empty cert_common_name value in subject '{}'",
                    config_subject.id
                );
            }
            if config_subject.usernames.as_ref().is_some_and(|usernames| {
                usernames
                    .iter()
                    .any(|username| username.0.trim().is_empty())
            }) {
                bail!(
                    "Found empty username value in subject '{}'",
                    config_subject.id
                );
            }
            // Map properties to SubjectProperty type
            // FIXME: Unnecessary .collect() because of different iterator types
            let interfaces = config_subject
                .interfaces
                .map(|interfaces| {
                    interfaces
                        .into_iter()
                        .map(SubjectProperty::Exactly)
                        .collect::<Vec<_>>()
                })
                .unwrap_or(vec![SubjectProperty::Wildcard]);
            // FIXME: Unnecessary .collect() because of different iterator types
            let cert_common_names = config_subject
                .cert_common_names
                .map(|cert_common_names| {
                    cert_common_names
                        .into_iter()
                        .map(SubjectProperty::Exactly)
                        .collect::<Vec<_>>()
                })
                .unwrap_or(vec![SubjectProperty::Wildcard]);
            // FIXME: Unnecessary .collect() because of different iterator types
            let usernames = config_subject
                .usernames
                .map(|usernames| {
                    usernames
                        .into_iter()
                        .map(SubjectProperty::Exactly)
                        .collect::<Vec<_>>()
                })
                .unwrap_or(vec![SubjectProperty::Wildcard]);
            // FIXME: Unnecessary .collect() because of different iterator types
            let link_types = config_subject
                .link_protocols
                .map(|link_types| {
                    link_types
                        .into_iter()
                        .map(SubjectProperty::Exactly)
                        .collect::<Vec<_>>()
                })
                .unwrap_or(vec![SubjectProperty::Wildcard]);
            // FIXME: Unnecessary .collect() because of different iterator types
            let zids = config_subject
                .zids
                .map(|zids| {
                    zids.into_iter()
                        .map(SubjectProperty::Exactly)
                        .collect::<Vec<_>>()
                })
                .unwrap_or(vec![SubjectProperty::Wildcard]);

            // create ACL subject combinations
            let subject_combination_ids = interfaces
                .into_iter()
                .cartesian_product(cert_common_names)
                .cartesian_product(usernames)
                .cartesian_product(link_types)
                .cartesian_product(zids)
                .map(
                    |((((interface, cert_common_name), username), link_type), zid)| {
                        let subject = Subject {
                            interface,
                            cert_common_name,
                            username,
                            link_type,
                            zid,
                        };
                        subject_map_builder.insert_or_get(subject)
                    },
                )
                .collect();
            subject_id_map.insert(config_subject.id.clone(), subject_combination_ids);
        }
        // finally, handle policy content
        for (entry_id, entry) in policies.iter().enumerate() {
            if let Some(policy_custom_id) = &entry.id {
                if !policy_id_set.insert(policy_custom_id.clone()) {
                    bail!(
                        "Policy id must be unique: id '{}' is repeated",
                        policy_custom_id
                    );
                }
            }
            // validate policy config lists
            if entry.rules.is_empty() || entry.subjects.is_empty() {
                bail!(
                    "Policy #{} is malformed: empty subjects or rules list",
                    entry_id
                );
            }
            for subject_config_id in &entry.subjects {
                if subject_config_id.trim().is_empty() {
                    bail!("Found empty subject id in policy #{}", entry_id)
                }
                if !subject_id_map.contains_key(subject_config_id) {
                    bail!(
                        "Subject '{}' in policy #{} does not exist in subjects list",
                        subject_config_id,
                        entry_id
                    )
                }
            }
            // Create PolicyRules
            for rule_id in &entry.rules {
                if rule_id.trim().is_empty() {
                    bail!("Found empty rule id in policy #{}", entry_id)
                }
                let rule = rule_map.get(rule_id).ok_or(zerror!(
                    "Rule '{}' in policy #{} does not exist in rules list",
                    rule_id,
                    entry_id
                ))?;
                for subject_config_id in &entry.subjects {
                    let subject_combination_ids = subject_id_map
                        .get(subject_config_id)
                        .expect("config subject id should exist in subject_id_map");
                    for subject_id in subject_combination_ids {
                        for flow in rule
                            .flows
                            .as_ref()
                            .expect("flows list should be defined in rule")
                        {
                            for message in &rule.messages {
                                for key_expr in &rule.key_exprs {
                                    policy_rules.push(PolicyRule {
                                        subject_id: *subject_id,
                                        key_expr: key_expr.clone(),
                                        message: *message,
                                        permission: rule.permission,
                                        flow: *flow,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(PolicyInformation {
            subject_map: subject_map_builder.build(),
            policy_rules,
        })
    }

    /// Return the default permission, but deny if the key is outside the namespace.
    /// Priority: namespace deny > default permission.
    pub(crate) fn namespace_aware_default(&self, key_expr: &keyexpr) -> Permission {
        if !self.is_under_namespace(key_expr) {
            return Permission::Deny;
        }
        self.default_permission
    }

    /// Check each msg against the ACL ruleset for allow/deny.
    pub fn policy_decision_point(
        &self,
        subject: usize,
        flow: InterceptorFlow,
        message: AclMessage,
        key_expr: &keyexpr,
    ) -> ZResult<Permission> {
        let policy_map = &self.policy_map;
        if policy_map.is_empty() {
            return Ok(self.namespace_aware_default(key_expr));
        }
        match policy_map.get(&subject) {
            Some(single_policy) => {
                // Priority: explicit deny > explicit allow > namespace deny > default permission
                let deny_result = single_policy
                    .flow(flow)
                    .action(message)
                    .deny
                    .nodes_including(key_expr)
                    .any(|n| n.weight().is_some());
                if deny_result {
                    return Ok(Permission::Deny);
                }
                if self.default_permission == Permission::Allow && self.namespace.is_none() {
                    // Fast path: default Allow, no namespace — no need to check allow tree
                    Ok(Permission::Allow)
                } else {
                    // Check explicit allow tree — explicit allow overrides namespace deny
                    let allow_result = single_policy
                        .flow(flow)
                        .action(message)
                        .allow
                        .nodes_including(key_expr)
                        .any(|n| n.weight().is_some());

                    if allow_result {
                        Ok(Permission::Allow)
                    } else {
                        // No explicit allow — apply namespace deny then default permission
                        Ok(self.namespace_aware_default(key_expr))
                    }
                }
            }
            None => Ok(self.namespace_aware_default(key_expr)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zenoh_config::{AclMessage, InterceptorFlow, Permission};
    use zenoh_keyexpr::keyexpr;

    /// Helper to create a PolicyEnforcer with namespace set.
    /// This will NOT compile until the `namespace` field is added to `PolicyEnforcer` (Red Phase).
    fn enforcer_with_namespace(ns: &str, default_perm: Permission) -> PolicyEnforcer {
        let mut enforcer = PolicyEnforcer::new();
        enforcer.namespace = Some(
            zenoh_keyexpr::OwnedNonWildKeyExpr::try_from(ns.to_string())
                .expect("test namespace should be valid"),
        );
        enforcer.default_permission = default_perm;
        enforcer
    }

    #[test]
    fn namespace_denies_key_outside_namespace() {
        // Router with namespace "tenant-a", default allow
        // Key "tenant-b/data" is OUTSIDE namespace -> should be DENIED by auto-deny
        let enforcer = enforcer_with_namespace("tenant-a", Permission::Allow);
        let result = enforcer
            .policy_decision_point(
                0, // no matching subject in empty policy_map -> would normally return default_permission
                InterceptorFlow::Ingress,
                AclMessage::Put,
                keyexpr::new("tenant-b/data").unwrap(),
            )
            .unwrap();
        assert_eq!(
            result,
            Permission::Deny,
            "key outside namespace should be denied"
        );
    }

    #[test]
    fn namespace_allows_key_inside_namespace() {
        // Router with namespace "tenant-a", default allow
        // Key "tenant-a/data" is INSIDE namespace -> should use default permission (Allow)
        let enforcer = enforcer_with_namespace("tenant-a", Permission::Allow);
        let result = enforcer
            .policy_decision_point(
                0,
                InterceptorFlow::Ingress,
                AclMessage::Put,
                keyexpr::new("tenant-a/data").unwrap(),
            )
            .unwrap();
        assert_eq!(
            result,
            Permission::Allow,
            "key inside namespace should be allowed"
        );
    }

    #[test]
    fn namespace_allows_exact_namespace_key() {
        // Key exactly matching the namespace (no trailing slash) should be allowed
        let enforcer = enforcer_with_namespace("tenant-a", Permission::Allow);
        let result = enforcer
            .policy_decision_point(
                0,
                InterceptorFlow::Ingress,
                AclMessage::Put,
                keyexpr::new("tenant-a").unwrap(),
            )
            .unwrap();
        assert_eq!(
            result,
            Permission::Allow,
            "exact namespace key should be allowed"
        );
    }

    #[test]
    fn no_namespace_no_change() {
        // No namespace configured -> default permission applies, no auto-deny
        let mut enforcer = PolicyEnforcer::new();
        enforcer.default_permission = Permission::Allow;
        let result = enforcer
            .policy_decision_point(
                0,
                InterceptorFlow::Ingress,
                AclMessage::Put,
                keyexpr::new("any/key/works").unwrap(),
            )
            .unwrap();
        assert_eq!(
            result,
            Permission::Allow,
            "no namespace should mean no auto-deny"
        );
    }

    #[test]
    fn namespace_applies_to_all_message_types() {
        // Auto-deny should apply to all message types, not just Put
        let enforcer = enforcer_with_namespace("ns1", Permission::Allow);
        for msg_type in [
            AclMessage::Put,
            AclMessage::Delete,
            AclMessage::Query,
            AclMessage::Reply,
            AclMessage::DeclareSubscriber,
            AclMessage::DeclareQueryable,
            AclMessage::LivelinessToken,
            AclMessage::DeclareLivelinessSubscriber,
            AclMessage::LivelinessQuery,
        ] {
            let result = enforcer
                .policy_decision_point(
                    0,
                    InterceptorFlow::Ingress,
                    msg_type,
                    keyexpr::new("other/key").unwrap(),
                )
                .unwrap();
            assert_eq!(
                result,
                Permission::Deny,
                "namespace auto-deny should apply to {:?}",
                msg_type
            );
        }
    }

    #[test]
    fn namespace_applies_to_both_flows() {
        // Auto-deny should apply to both ingress and egress
        let enforcer = enforcer_with_namespace("ns1", Permission::Allow);
        for flow in [InterceptorFlow::Ingress, InterceptorFlow::Egress] {
            let result = enforcer
                .policy_decision_point(
                    0,
                    flow,
                    AclMessage::Put,
                    keyexpr::new("other/key").unwrap(),
                )
                .unwrap();
            assert_eq!(
                result,
                Permission::Deny,
                "namespace auto-deny should apply to {:?}",
                flow
            );
        }
    }

    #[test]
    fn explicit_allow_overrides_namespace_deny() {
        // Set up: namespace "ns1", default deny, explicit allow rule for "other/data"
        // Key "other/data" is OUTSIDE namespace but has explicit allow -> should ALLOW
        // Priority: explicit deny > explicit allow > namespace deny > default permission
        let mut enforcer = PolicyEnforcer::new();
        enforcer.namespace = Some(
            zenoh_keyexpr::OwnedNonWildKeyExpr::try_from("ns1".to_string())
                .expect("test namespace should be valid"),
        );
        enforcer.acl_enabled = true;
        enforcer.default_permission = Permission::Deny;

        // Build a minimal ACL config with an explicit allow for "other/data"
        let acl_config = zenoh_config::AclConfig {
            enabled: true,
            default_permission: Permission::Deny,
            rules: Some(vec![AclConfigRule {
                id: "allow-cross-ns".to_string(),
                permission: Permission::Allow,
                flows: Some(
                    vec![InterceptorFlow::Ingress, InterceptorFlow::Egress]
                        .try_into()
                        .unwrap(),
                ),
                messages: vec![AclMessage::Put].try_into().unwrap(),
                key_exprs: vec!["other/data".try_into().unwrap()].try_into().unwrap(),
            }]),
            subjects: Some(vec![AclConfigSubjects {
                id: "all".to_string(),
                interfaces: None,
                cert_common_names: None,
                usernames: None,
                link_protocols: None,
                zids: None,
            }]),
            policies: Some(vec![AclConfigPolicyEntry {
                id: None,
                rules: vec!["allow-cross-ns".to_string()],
                subjects: vec!["all".to_string()],
            }]),
        };

        // Re-init with ACL config (preserving namespace)
        enforcer.init(&acl_config).unwrap();

        // Find the subject ID for "all"
        let subject_query = SubjectQuery {
            interface: None,
            cert_common_name: None,
            username: None,
            link_protocol: None,
            zid: None,
        };
        let subject_id = enforcer
            .subject_store
            .query(&subject_query)
            .next()
            .expect("should have a subject")
            .id;

        let result = enforcer
            .policy_decision_point(
                subject_id,
                InterceptorFlow::Ingress,
                AclMessage::Put,
                keyexpr::new("other/data").unwrap(),
            )
            .unwrap();
        assert_eq!(
            result,
            Permission::Allow,
            "explicit allow should override namespace auto-deny"
        );
    }

    #[test]
    fn explicit_deny_still_wins_over_namespace() {
        // Explicit deny should still take precedence (existing behavior preserved)
        let mut enforcer = PolicyEnforcer::new();
        enforcer.namespace = Some(
            zenoh_keyexpr::OwnedNonWildKeyExpr::try_from("ns1".to_string())
                .expect("test namespace should be valid"),
        );
        enforcer.acl_enabled = true;
        enforcer.default_permission = Permission::Allow;

        let acl_config = zenoh_config::AclConfig {
            enabled: true,
            default_permission: Permission::Allow,
            rules: Some(vec![AclConfigRule {
                id: "deny-inside".to_string(),
                permission: Permission::Deny,
                flows: Some(
                    vec![InterceptorFlow::Ingress, InterceptorFlow::Egress]
                        .try_into()
                        .unwrap(),
                ),
                messages: vec![AclMessage::Put].try_into().unwrap(),
                key_exprs: vec!["ns1/secret".try_into().unwrap()].try_into().unwrap(),
            }]),
            subjects: Some(vec![AclConfigSubjects {
                id: "all".to_string(),
                interfaces: None,
                cert_common_names: None,
                usernames: None,
                link_protocols: None,
                zids: None,
            }]),
            policies: Some(vec![AclConfigPolicyEntry {
                id: None,
                rules: vec!["deny-inside".to_string()],
                subjects: vec!["all".to_string()],
            }]),
        };

        enforcer.init(&acl_config).unwrap();

        let subject_query = SubjectQuery {
            interface: None,
            cert_common_name: None,
            username: None,
            link_protocol: None,
            zid: None,
        };
        let subject_id = enforcer
            .subject_store
            .query(&subject_query)
            .next()
            .expect("should have a subject")
            .id;

        // "ns1/secret" is inside namespace but has explicit deny -> DENY
        let result = enforcer
            .policy_decision_point(
                subject_id,
                InterceptorFlow::Ingress,
                AclMessage::Put,
                keyexpr::new("ns1/secret").unwrap(),
            )
            .unwrap();
        assert_eq!(
            result,
            Permission::Deny,
            "explicit deny should still take precedence"
        );
    }
}
