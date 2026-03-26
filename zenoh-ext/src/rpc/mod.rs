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

mod client;
mod deadline;
mod discovery;
mod error;
mod server;

pub use client::ServiceClient;
pub use deadline::{deadline_attachment, DeadlineContext, DEADLINE_ATTACHMENT_KEY};
pub use error::{ServiceError, StatusCode};
pub use server::{MethodHandler, ServiceServer, ServiceServerBuilder, METHOD_ATTACHMENT_KEY};
