//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
use super::Runtime;
use crate::plugins::PluginsMgr;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use async_trait::async_trait;
use futures::future;
use futures::future::{BoxFuture, FutureExt};
use log::{error, trace};
use serde_json::json;
use std::collections::HashMap;
use zenoh_protocol::{
    core::{
        queryable::EVAL, rname, CongestionControl, PeerId, QueryConsolidation, QueryTarget,
        Reliability, ResKey, SubInfo, ZInt,
    },
    io::RBuf,
    proto::{encoding, DataInfo, RoutingContext},
    session::Primitives,
};

type Handler = Box<dyn Fn(&AdminSpace) -> BoxFuture<'_, (RBuf, ZInt)> + Send + Sync>;

pub struct AdminSpace {
    runtime: Runtime,
    plugins_mgr: PluginsMgr,
    primitives: Mutex<Option<Arc<dyn Primitives + Send + Sync>>>,
    mappings: Mutex<HashMap<ZInt, String>>,
    pid_str: String,
    handlers: HashMap<String, Handler>,
}

impl AdminSpace {
    pub async fn start(runtime: &Runtime, plugins_mgr: PluginsMgr) {
        let pid_str = runtime.get_pid_str().await;
        let root_path = format!("/@/router/{}", pid_str);

        let mut handlers: HashMap<String, Handler> = HashMap::new();
        handlers.insert(
            root_path.clone(),
            Box::new(|admin| AdminSpace::router_data(admin).boxed()),
        );
        handlers.insert(
            [&root_path, "/linkstate/routers"].concat(),
            Box::new(|admin| AdminSpace::linkstate_routers_data(admin).boxed()),
        );
        handlers.insert(
            [&root_path, "/linkstate/peers"].concat(),
            Box::new(|admin| AdminSpace::linkstate_peers_data(admin).boxed()),
        );

        let admin = Arc::new(AdminSpace {
            runtime: runtime.clone(),
            plugins_mgr,
            primitives: Mutex::new(None),
            mappings: Mutex::new(HashMap::new()),
            pid_str,
            handlers,
        });

        let primitives = runtime
            .read()
            .await
            .router
            .new_primitives(admin.clone())
            .await;
        admin.primitives.lock().await.replace(primitives.clone());

        primitives
            .queryable(&[&root_path, "/**"].concat().into(), None)
            .await;
    }

    pub async fn reskey_to_string(&self, key: &ResKey) -> Option<String> {
        match key {
            ResKey::RId(id) => self.mappings.lock().await.get(&id).cloned(),
            ResKey::RIdWithSuffix(id, suffix) => self
                .mappings
                .lock()
                .await
                .get(&id)
                .map(|prefix| format!("{}{}", prefix, suffix)),
            ResKey::RName(name) => Some(name.clone()),
        }
    }

    pub async fn router_data(&self) -> (RBuf, ZInt) {
        let session_mgr = &self.runtime.read().await.orchestrator.manager;

        // plugins info
        let plugins: Vec<serde_json::Value> = self
            .plugins_mgr
            .plugins
            .iter()
            .map(|plugin| {
                json!({
                    "name": plugin.name,
                    "path": plugin.path
                })
            })
            .collect();

        // locators info
        let locators: Vec<serde_json::Value> = session_mgr
            .get_locators()
            .await
            .iter()
            .map(|locator| json!(locator.to_string()))
            .collect();

        // sessions info
        let sessions = future::join_all(session_mgr.get_sessions().await.iter().map(async move |session|
            json!({
                "peer": session.get_pid().map_or_else(|_| "unavailable".to_string(), |p| p.to_string()),
                "links": session.get_links().await.map_or_else(
                    |_| vec!(),
                    |links| links.iter().map(|link| link.get_dst().to_string()).collect()
                )
            })
        )).await;

        let json = json!({
            "pid": self.pid_str,
            "locators": locators,
            "sessions": sessions,
            "plugins": plugins,
        });
        log::trace!("AdminSpace router_data: {:?}", json);
        (RBuf::from(json.to_string().as_bytes()), encoding::APP_JSON)
    }

    pub async fn linkstate_routers_data(&self) -> (RBuf, ZInt) {
        (
            RBuf::from(
                self.runtime
                    .read()
                    .await
                    .router
                    .routers_net
                    .as_ref()
                    .unwrap()
                    .read()
                    .await
                    .dot()
                    .as_bytes(),
            ),
            encoding::TEXT_PLAIN,
        )
    }

    pub async fn linkstate_peers_data(&self) -> (RBuf, ZInt) {
        (
            RBuf::from(
                self.runtime
                    .read()
                    .await
                    .router
                    .peers_net
                    .as_ref()
                    .unwrap()
                    .read()
                    .await
                    .dot()
                    .as_bytes(),
            ),
            encoding::TEXT_PLAIN,
        )
    }
}

#[async_trait]
impl Primitives for AdminSpace {
    async fn resource(&self, rid: ZInt, reskey: &ResKey) {
        trace!("recv Resource {} {:?}", rid, reskey);
        match self.reskey_to_string(reskey).await {
            Some(s) => {
                self.mappings.lock().await.insert(rid, s);
            }
            None => error!("Unknown rid {}!", rid),
        }
    }

    async fn forget_resource(&self, _rid: ZInt) {
        trace!("recv Forget Resource {}", _rid);
    }

    async fn publisher(&self, _reskey: &ResKey, _routing_context: Option<RoutingContext>) {
        trace!("recv Publisher {:?}", _reskey);
    }

    async fn forget_publisher(&self, _reskey: &ResKey, _routing_context: Option<RoutingContext>) {
        trace!("recv Forget Publisher {:?}", _reskey);
    }

    async fn subscriber(
        &self,
        _reskey: &ResKey,
        _sub_info: &SubInfo,
        _routing_context: Option<RoutingContext>,
    ) {
        trace!("recv Subscriber {:?} , {:?}", _reskey, _sub_info);
    }

    async fn forget_subscriber(&self, _reskey: &ResKey, _routing_context: Option<RoutingContext>) {
        trace!("recv Forget Subscriber {:?}", _reskey);
    }

    async fn queryable(&self, _reskey: &ResKey, _routing_context: Option<RoutingContext>) {
        trace!("recv Queryable {:?}", _reskey);
    }

    async fn forget_queryable(&self, _reskey: &ResKey, _routing_context: Option<RoutingContext>) {
        trace!("recv Forget Queryable {:?}", _reskey);
    }

    async fn data(
        &self,
        reskey: &ResKey,
        payload: RBuf,
        reliability: Reliability,
        congestion_control: CongestionControl,
        data_info: Option<DataInfo>,
        _routing_context: Option<RoutingContext>,
    ) {
        trace!(
            "recv Data {:?} {:?} {:?} {:?} {:?}",
            reskey,
            payload,
            reliability,
            congestion_control,
            data_info,
        );
    }

    async fn query(
        &self,
        reskey: &ResKey,
        predicate: &str,
        qid: ZInt,
        target: QueryTarget,
        _consolidation: QueryConsolidation,
        _routing_context: Option<RoutingContext>,
    ) {
        trace!(
            "recv Query {:?} {:?} {:?} {:?}",
            reskey,
            predicate,
            target,
            _consolidation
        );

        let primitives = self.primitives.lock().await.as_ref().unwrap().clone();
        let replier_id = self.runtime.read().await.pid.clone(); // @TODO build/use prebuilt specific pid

        let mut replies = vec![];
        match self.reskey_to_string(reskey).await {
            Some(name) => {
                for (path, handler) in &self.handlers {
                    if rname::intersect(&name, path) {
                        let (payload, encoding) = handler(self).await;
                        replies.push((
                            ResKey::RName(path.clone()),
                            payload,
                            Some(DataInfo {
                                source_id: None,
                                source_sn: None,
                                first_router_id: None,
                                first_router_sn: None,
                                timestamp: None,
                                kind: None,
                                encoding: Some(encoding),
                            }),
                        ));
                    }
                }
            }
            None => error!("Unknown ResKey!!"),
        }

        // router is not re-entrant
        task::spawn(async move {
            for (reskey, payload, data_info) in replies {
                primitives
                    .reply_data(qid, EVAL, replier_id.clone(), reskey, data_info, payload)
                    .await;
            }
            primitives.reply_final(qid).await;
        });
    }

    async fn reply_data(
        &self,
        qid: ZInt,
        source_kind: ZInt,
        replier_id: PeerId,
        reskey: ResKey,
        info: Option<DataInfo>,
        payload: RBuf,
    ) {
        trace!(
            "recv ReplyData {:?} {:?} {:?} {:?} {:?} {:?}",
            qid,
            source_kind,
            replier_id,
            reskey,
            info,
            payload
        );
    }

    async fn reply_final(&self, qid: ZInt) {
        trace!("recv ReplyFinal {:?}", qid);
    }

    async fn pull(
        &self,
        _is_final: bool,
        _reskey: &ResKey,
        _pull_id: ZInt,
        _max_samples: &Option<ZInt>,
    ) {
        trace!(
            "recv Pull {:?} {:?} {:?} {:?}",
            _is_final,
            _reskey,
            _pull_id,
            _max_samples
        );
    }

    async fn close(&self) {
        trace!("recv Close");
    }
}
