//
// Copyright (c) 2023 ZettaScale Technology
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
use zenoh::prelude::r#async::*;
use zenoh_core::zasync_executor_init;

const TIMEOUT: Duration = Duration::from_secs(30);

macro_rules! ztimeout {
    ($f:expr) => {
        async_std::future::timeout(TIMEOUT, $f).await.unwrap()
    };
}

#[test]
fn linger_config_session() {
    let _ = env_logger::try_init();
    async_std::task::block_on(async {
        zasync_executor_init!();
        let mut config = config::peer();
        config
            .insert_json5("transport/link/tcp", r#"{ so_linger: 5 }"#)
            .unwrap();
        config
            .insert_json5("listen/endpoints", r#"["tcp/[::1]:0"]"#)
            .unwrap();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();

        let session = ztimeout!(zenoh::open(config).res_async()).unwrap();
        ztimeout!(session.close().res_async()).unwrap();
    });
}

#[test]
fn linger_config_endpoint() {
    let _ = env_logger::try_init();
    async_std::task::block_on(async {
        zasync_executor_init!();
        let mut config = config::peer();
        config
            .insert_json5(
                "listen/endpoints",
                r#"["tcp/[::1]:0#so_linger=5"]"#,
            )
            .unwrap();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();

        let session = ztimeout!(zenoh::open(config).res_async()).unwrap();
        ztimeout!(session.close().res_async()).unwrap();
    });
}

#[test]
fn linger_config_session_overwritten_by_endpoint() {
    let _ = env_logger::try_init();
    async_std::task::block_on(async {
        zasync_executor_init!();
        let mut config = config::peer();
        config
            .insert_json5("transport/link/tcp", r#"{ so_linger: 10 }"#)
            .unwrap();
        config
            .insert_json5(
                "listen/endpoints",
                r#"["tcp/[::1]:0#so_linger=5"]"#,
            )
            .unwrap();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();

        let session = ztimeout!(zenoh::open(config).res_async()).unwrap();
        ztimeout!(session.close().res_async()).unwrap();
    });
}
