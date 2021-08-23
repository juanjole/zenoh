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
//
use super::super::TransportManager;
use super::authenticator::*;
use super::defaults::*;
use super::protocol::core::{PeerId, WhatAmI, ZInt};
use super::transport::{TransportUnicastInner, TransportUnicastInnerConfig};
use super::*;
use crate::net::link::*;
use async_std::prelude::*;
use async_std::sync::{Arc as AsyncArc, Mutex as AsyncMutex};
use async_std::task;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use zenoh_util::core::{ZError, ZErrorKind, ZResult};
use zenoh_util::properties::config::ConfigProperties;
use zenoh_util::properties::config::*;
use zenoh_util::{zasynclock, zerror, zlock};

pub struct TransportManagerConfigUnicast {
    pub lease: ZInt,
    pub keep_alive: ZInt,
    pub open_timeout: ZInt,
    pub open_pending: usize,
    pub max_transports: usize,
    pub max_links: usize,
    pub peer_authenticator: Vec<PeerAuthenticator>,
    pub link_authenticator: Vec<LinkAuthenticator>,
}

impl Default for TransportManagerConfigUnicast {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl TransportManagerConfigUnicast {
    pub fn builder() -> TransportManagerConfigBuilderUnicast {
        TransportManagerConfigBuilderUnicast::default()
    }
}

pub struct TransportManagerConfigBuilderUnicast {
    lease: ZInt,
    keep_alive: ZInt,
    open_timeout: ZInt,
    open_pending: usize,
    max_transports: usize,
    max_links: usize,
    peer_authenticator: Vec<PeerAuthenticator>,
    link_authenticator: Vec<LinkAuthenticator>,
}

impl Default for TransportManagerConfigBuilderUnicast {
    fn default() -> TransportManagerConfigBuilderUnicast {
        TransportManagerConfigBuilderUnicast {
            lease: *ZN_LINK_LEASE,
            keep_alive: *ZN_LINK_KEEP_ALIVE,
            open_timeout: *ZN_OPEN_TIMEOUT,
            open_pending: *ZN_OPEN_INCOMING_PENDING,
            max_transports: usize::MAX,
            max_links: usize::MAX,
            peer_authenticator: vec![DummyPeerAuthenticator::make()],
            link_authenticator: vec![DummyLinkAuthenticator::make()],
        }
    }
}

impl TransportManagerConfigBuilderUnicast {
    pub fn lease(mut self, lease: ZInt) -> Self {
        self.lease = lease;
        self
    }

    pub fn keep_alive(mut self, keep_alive: ZInt) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    pub fn open_timeout(mut self, open_timeout: ZInt) -> Self {
        self.open_timeout = open_timeout;
        self
    }

    pub fn open_pending(mut self, open_pending: usize) -> Self {
        self.open_pending = open_pending;
        self
    }

    pub fn max_transports(mut self, max_transports: usize) -> Self {
        self.max_transports = max_transports;
        self
    }

    pub fn max_links(mut self, max_links: usize) -> Self {
        self.max_links = max_links;
        self
    }

    pub fn peer_authenticator(mut self, peer_authenticator: Vec<PeerAuthenticator>) -> Self {
        self.peer_authenticator = peer_authenticator;
        self
    }

    pub fn link_authenticator(mut self, link_authenticator: Vec<LinkAuthenticator>) -> Self {
        self.link_authenticator = link_authenticator;
        self
    }

    pub async fn from_properties(
        mut self,
        properties: &ConfigProperties,
    ) -> ZResult<TransportManagerConfigBuilderUnicast> {
        macro_rules! zparse {
            ($str:expr) => {
                $str.parse().map_err(|_| {
                    let e = format!(
                        "Failed to read configuration: {} is not a valid value",
                        $str
                    );
                    log::warn!("{}", e);
                    zerror2!(ZErrorKind::ValueDecodingFailed { descr: e })
                })
            };
        }

        if let Some(v) = properties.get(&ZN_LINK_LEASE_KEY) {
            self = self.lease(zparse!(v)?);
        }
        if let Some(v) = properties.get(&ZN_LINK_KEEP_ALIVE_KEY) {
            self = self.keep_alive(zparse!(v)?);
        }
        if let Some(v) = properties.get(&ZN_OPEN_TIMEOUT_KEY) {
            self = self.open_timeout(zparse!(v)?);
        }
        if let Some(v) = properties.get(&ZN_OPEN_INCOMING_PENDING_KEY) {
            self = self.open_pending(zparse!(v)?);
        }
        if let Some(v) = properties.get(&ZN_MAX_SESSIONS_KEY) {
            self = self.max_transports(zparse!(v)?);
        }
        if let Some(v) = properties.get(&ZN_MAX_LINKS_KEY) {
            self = self.max_links(zparse!(v)?);
        }

        self = self.peer_authenticator(PeerAuthenticator::from_properties(properties).await?);
        self = self.link_authenticator(LinkAuthenticator::from_properties(properties).await?);

        Ok(self)
    }

    pub fn build(self) -> TransportManagerConfigUnicast {
        TransportManagerConfigUnicast {
            lease: self.lease,
            keep_alive: self.keep_alive,
            open_timeout: self.open_timeout,
            open_pending: self.open_pending,
            max_transports: self.max_transports,
            max_links: self.max_links,
            peer_authenticator: self.peer_authenticator,
            link_authenticator: self.link_authenticator,
        }
    }
}

pub struct TransportManagerStateUnicast {
    // Outgoing and incoming opened (i.e. established) transports
    pub(super) opened: AsyncArc<AsyncMutex<HashMap<PeerId, Opened>>>,
    // Incoming uninitialized transports
    pub(super) incoming: AsyncArc<AsyncMutex<HashMap<Link, Option<Vec<u8>>>>>,
    // Established listeners
    pub(super) protocols: Arc<Mutex<HashMap<LocatorProtocol, LinkManagerUnicast>>>,
    // Established transports
    pub(super) transports: Arc<Mutex<HashMap<PeerId, Arc<TransportUnicastInner>>>>,
}

impl Default for TransportManagerStateUnicast {
    fn default() -> TransportManagerStateUnicast {
        TransportManagerStateUnicast {
            opened: AsyncArc::new(AsyncMutex::new(HashMap::new())),
            incoming: AsyncArc::new(AsyncMutex::new(HashMap::new())),
            protocols: Arc::new(Mutex::new(HashMap::new())),
            transports: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub(super) struct Opened {
    pub(super) whatami: WhatAmI,
    pub(super) sn_resolution: ZInt,
    pub(super) initial_sn: ZInt,
}

impl TransportManager {
    /*************************************/
    /*            LINK MANAGER           */
    /*************************************/
    async fn get_or_new_link_manager_unicast(
        &self,
        protocol: &LocatorProtocol,
    ) -> LinkManagerUnicast {
        loop {
            match self.get_link_manager_unicast(protocol) {
                Ok(manager) => return manager,
                Err(_) => match self.new_link_manager_unicast(protocol).await {
                    Ok(manager) => return manager,
                    Err(_) => continue,
                },
            }
        }
    }

    async fn new_link_manager_unicast(
        &self,
        protocol: &LocatorProtocol,
    ) -> ZResult<LinkManagerUnicast> {
        let mut w_guard = zlock!(self.state.unicast.protocols);
        if w_guard.contains_key(protocol) {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    "Can not create the link manager for protocol ({}) because it already exists",
                    protocol
                )
            });
        }

        let lm = LinkManagerBuilderUnicast::make(self.clone(), protocol)?;
        w_guard.insert(protocol.clone(), lm.clone());
        Ok(lm)
    }

    fn get_link_manager_unicast(&self, protocol: &LocatorProtocol) -> ZResult<LinkManagerUnicast> {
        match zlock!(self.state.unicast.protocols).get(protocol) {
            Some(manager) => Ok(manager.clone()),
            None => zerror!(ZErrorKind::Other {
                descr: format!(
                    "Can not get the link manager for protocol ({}) because it has not been found",
                    protocol
                )
            }),
        }
    }

    async fn del_link_manager_unicast(&self, protocol: &LocatorProtocol) -> ZResult<()> {
        match zlock!(self.state.unicast.protocols).remove(protocol) {
            Some(lm) => {
                let mut listeners = lm.get_listeners();
                for l in listeners.drain(..) {
                    let _ = lm.del_listener(&l).await;
                }
                Ok(())
            },
            None => zerror!(ZErrorKind::Other {
                descr: format!("Can not delete the link manager for protocol ({}) because it has not been found.", protocol)
            })
        }
    }

    /*************************************/
    /*              LISTENER             */
    /*************************************/
    pub async fn add_listener_unicast(&self, locator: &Locator) -> ZResult<Locator> {
        let manager = self
            .get_or_new_link_manager_unicast(&locator.get_proto())
            .await;
        let ps = self.config.locator_property.get(&locator.get_proto());
        manager.new_listener(locator, ps).await
    }

    pub async fn del_listener_unicast(&self, locator: &Locator) -> ZResult<()> {
        let lm = self.get_link_manager_unicast(&locator.get_proto())?;
        lm.del_listener(locator).await?;
        if lm.get_listeners().is_empty() {
            self.del_link_manager_unicast(&locator.get_proto()).await?;
        }
        Ok(())
    }

    pub fn get_listeners_unicast(&self) -> Vec<Locator> {
        let mut vec: Vec<Locator> = vec![];
        for p in zlock!(self.state.unicast.protocols).values() {
            vec.extend_from_slice(&p.get_listeners());
        }
        vec
    }

    pub fn get_locators_unicast(&self) -> Vec<Locator> {
        let mut vec: Vec<Locator> = vec![];
        for p in zlock!(self.state.unicast.protocols).values() {
            vec.extend_from_slice(&p.get_locators());
        }
        vec
    }

    /*************************************/
    /*             TRANSPORT             */
    /*************************************/
    pub(super) fn init_transport_unicast(
        &self,
        config: TransportConfigUnicast,
    ) -> ZResult<TransportUnicast> {
        let mut guard = zlock!(self.state.unicast.transports);

        // First verify if the transport already exists
        if let Some(transport) = guard.get(&config.peer) {
            if transport.whatami != config.whatami {
                let e = format!(
                    "Transport with peer {} already exist. Invalid whatami: {}. Execpted: {}.",
                    config.peer, config.whatami, transport.whatami
                );
                log::trace!("{}", e);
                return zerror!(ZErrorKind::Other { descr: e });
            }

            if transport.sn_resolution != config.sn_resolution {
                let e = format!(
                    "Transport with peer {} already exist. Invalid sn resolution: {}. Execpted: {}.",
                    config.peer, config.sn_resolution, transport.sn_resolution
                );
                log::trace!("{}", e);
                return zerror!(ZErrorKind::Other { descr: e });
            }

            if transport.is_shm != config.is_shm {
                let e = format!(
                    "Transport with peer {} already exist. Invalid is_shm: {}. Execpted: {}.",
                    config.peer, config.is_shm, transport.is_shm
                );
                log::trace!("{}", e);
                return zerror!(ZErrorKind::Other { descr: e });
            }

            return Ok(transport.into());
        }

        // Then verify that we haven't reached the transport number limit
        if guard.len() >= self.config.unicast.max_transports {
            let e = format!(
                "Max transports reached ({}). Denying new transport with peer: {}",
                self.config.unicast.max_transports, config.peer
            );
            log::trace!("{}", e);
            return zerror!(ZErrorKind::Other { descr: e });
        }

        // Create the transport transport
        let stc = TransportUnicastInnerConfig {
            manager: self.clone(),
            pid: config.peer.clone(),
            whatami: config.whatami,
            sn_resolution: config.sn_resolution,
            initial_sn_tx: config.initial_sn_tx,
            initial_sn_rx: config.initial_sn_rx,
            is_shm: config.is_shm,
            is_qos: config.is_qos,
        };
        let a_st = Arc::new(TransportUnicastInner::new(stc));

        // Create a weak reference to the transport transport
        let transport: TransportUnicast = (&a_st).into();
        // Add the transport transport to the list of active transports
        guard.insert(config.peer.clone(), a_st);

        log::debug!(
            "New transport opened with {}: whatami {}, sn resolution {}, initial sn tx {}, initial sn rx {}, shm: {}, qos: {}",
            config.peer,
            config.whatami,
            config.sn_resolution,
            config.initial_sn_tx,
            config.initial_sn_rx,
            config.is_shm,
            config.is_qos
        );

        Ok(transport)
    }

    pub async fn open_transport_unicast(&self, locator: &Locator) -> ZResult<TransportUnicast> {
        // Automatically create a new link manager for the protocol if it does not exist
        let manager = self
            .get_or_new_link_manager_unicast(&locator.get_proto())
            .await;
        let ps = self.config.locator_property.get(&locator.get_proto());
        // Create a new link associated by calling the Link Manager
        let link = manager.new_link(locator, ps).await?;
        // Open the link
        super::establishment::open_link(self, &link).await
    }

    pub fn get_transport_unicast(&self, peer: &PeerId) -> Option<TransportUnicast> {
        zlock!(self.state.unicast.transports)
            .get(peer)
            .map(|t| t.into())
    }

    pub fn get_transports_unicast(&self) -> Vec<TransportUnicast> {
        zlock!(self.state.unicast.transports)
            .values()
            .map(|t| t.into())
            .collect()
    }

    pub(super) async fn del_transport_unicast(&self, peer: &PeerId) -> ZResult<()> {
        let _ = zlock!(self.state.unicast.transports)
            .remove(peer)
            .ok_or_else(|| {
                let e = format!("Can not delete the transport of peer: {}", peer);
                log::trace!("{}", e);
                zerror2!(ZErrorKind::Other { descr: e })
            })?;

        for pa in self.config.unicast.peer_authenticator.iter() {
            pa.handle_close(peer).await;
        }
        Ok(())
    }

    pub(crate) async fn handle_new_link_unicast(
        &self,
        link: Link,
        properties: Option<LocatorProperty>,
    ) {
        let mut guard = zasynclock!(self.state.unicast.incoming);
        if guard.len() >= self.config.unicast.open_pending {
            // We reached the limit of concurrent incoming transport, this means two things:
            // - the values configured for ZN_OPEN_INCOMING_PENDING and ZN_OPEN_TIMEOUT
            //   are too small for the scenario zenoh is deployed in;
            // - there is a tentative of DoS attack.
            // In both cases, let's close the link straight away with no additional notification
            log::trace!("Closing link for preventing potential DoS: {}", link);
            let _ = link.close().await;
            return;
        }

        // A new link is available
        log::trace!("New link waiting... {}", link);
        guard.insert(link.clone(), None);
        drop(guard);

        let mut peer_id: Option<PeerId> = None;
        for la in self.config.unicast.link_authenticator.iter() {
            let res = la.handle_new_link(&link, properties.as_ref()).await;
            match res {
                Ok(pid) => {
                    // Check that all the peer authenticators, eventually return the same PeerId
                    if let Some(pid1) = peer_id.as_ref() {
                        if let Some(pid2) = pid.as_ref() {
                            if pid1 != pid2 {
                                log::debug!("Ambigous PeerID identification for link: {}", link);
                                let _ = link.close().await;
                                zasynclock!(self.state.unicast.incoming).remove(&link);
                                return;
                            }
                        }
                    } else {
                        peer_id = pid;
                    }
                }
                Err(e) => {
                    log::debug!("{}", e);
                    return;
                }
            }
        }

        // Spawn a task to accept the link
        let c_incoming = self.state.unicast.incoming.clone();
        let c_manager = self.clone();
        task::spawn(async move {
            let auth_link = AuthenticatedPeerLink {
                src: link.get_src(),
                dst: link.get_dst(),
                peer_id,
                properties,
            };

            let timeout = Duration::from_millis(c_manager.config.unicast.open_timeout);
            let res = super::establishment::accept_link(&c_manager, &link, &auth_link)
                .timeout(timeout)
                .await;
            match res {
                Ok(res) => {
                    if let Err(e) = res {
                        log::debug!("{}", e);
                    }
                }
                Err(e) => {
                    log::debug!("{}", e);
                    let _ = link.close().await;
                }
            }
            zasynclock!(c_incoming).remove(&link);
        });
    }
}