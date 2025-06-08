use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseMetaData},
};
use crate::erasure::{
    ec_service::EntryFragment,
    log_entry::{ECEntry, OperationType},
};
use crate::util::WRITE_ERROR_MSG;

use super::*;

impl<T, B> SequencePaxosEC<T, B>
where
    T: ECEntry,
    B: Storage<T, ClusterConfigEC>,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) fn handle_leader(&mut self, n: Ballot) {
        if n <= self.leader_state.n_leader || n <= self.internal_storage.get_promise() {
            return;
        }
        #[cfg(feature = "logging")]
        info!(self.logger, "Newly elected leader: {:?}", n);
        if self.pid == n.pid {
            self.leader_state =
                LeaderState::with(n, self.leader_state.max_pid, self.leader_state.quorum);
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage.set_promise(n).expect(WRITE_ERROR_MSG);
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_idx,
                accepted_idx,
                log_sync: None,
            };
            self.leader_state.set_promise(my_promise, self.pid, true);
            /* initialise longest chosen sequence and update state */
            self.state = (RoleEC::Leader, PhaseEC::Prepare);
            let prep = Prepare {
                n,
                decided_idx,
                n_accepted: na,
                accepted_idx,
            };
            /* send prepare */
            for pid in &self.peers {
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg: PaxosMsg::Prepare(prep),
                }));
            }
        } else {
            self.become_follower();
        }
    }

    pub(crate) fn become_follower(&mut self) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][NODE {}] ENTER become_follower", self.pid
        );
        #[cfg(feature = "logging")]
        info!(self.logger, "[NODE {}] become_follower", self.pid);
        self.state.0 = RoleEC::Follower;
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][NODE {}] ENTER handle_preparereq(from={})", self.pid, from
        );
        #[cfg(feature = "logging")]
        info!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == RoleEC::Leader && prepreq.n <= self.leader_state.n_leader {
            self.leader_state.reset_promise(from);
            self.leader_state.set_latest_accept_meta(from, None);
            self.send_prepare(from);
        }
    }

    pub(crate) fn handle_forwarded_proposal(&mut self, mut entries: Vec<T>) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][NODE {}] ENTER handle_forwarded_proposal: entries={:?}",
            self.pid,
            entries
                .iter()
                .map(|e| (e.key(), e.value().idx, &e.value().data))
                .collect::<Vec<_>>()
        );
        if !self.accepted_reconfiguration() {
            match self.state {
                (RoleEC::Leader, PhaseEC::Prepare) => self.buffered_proposals.append(&mut entries),
                (RoleEC::Leader, PhaseEC::Accept) => self.accept_entries_leader(entries),
                _ => self.forward_proposals(entries),
            }
        }
    }

    pub(crate) fn handle_forwarded_stopsign(&mut self, ss: StopSign<ClusterConfigEC>) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][NODE {}] ENTER handle_forwarded_stopsign: stopsign={:?}", self.pid, ss
        );
        if self.accepted_reconfiguration() {
            return;
        }
        match self.state {
            (RoleEC::Leader, PhaseEC::Prepare) => self.buffered_stopsign = Some(ss),
            (RoleEC::Leader, PhaseEC::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
    }

    pub(crate) fn send_prepare(&mut self, to: NodeId) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][NODE {}] ENTER send_prepare(to={})", self.pid, to
        );
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
            n_accepted: self.internal_storage.get_accepted_round(),
            accepted_idx: self.internal_storage.get_accepted_idx(),
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Prepare(prep),
        }));
    }

    pub(crate) fn accept_entry_leader(&mut self, entry: T) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER accept_entry_leader: key={}, op={:?}, fragment.idx={}, fragment.data={:?}",
            self.pid,
            entry.key(),
            entry.operation(),
            entry.value().idx,
            entry.value().data
        );
        let key = entry.key().to_string();
        let op = entry.operation().clone();
        let value_bytes =
            bincode::serialize(entry.value()).expect("ECEntry value must be serializable");
        let total_shards = self.peers.len() + 1;
        let fragments = self
            .ec_service
            .encode(&value_bytes)
            .expect("EC encode failed");
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] accept_entry_leader: encoded fragments={:?}",
            self.pid,
            fragments
                .iter()
                .map(|f| (f.idx, &f.data))
                .collect::<Vec<_>>()
        );
        // Assign fragment to self
        let my_idx = ECService::fragment_index_for_node(self.pid as usize, total_shards);
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] accept_entry_leader: my_idx={} (pid={})",
            self.pid,
            my_idx,
            self.pid
        );
        let my_entry = T::from_parts(key.clone(), fragments[my_idx].clone(), op.clone());
        let accepted_metadata = self
            .internal_storage
            .append_entry_with_batching(my_entry)
            .expect(WRITE_ERROR_MSG);

        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            // Distribute fragments to followers
            self.send_acceptdecide(key, op, fragments);
        }
    }

    pub(crate) fn accept_entries_leader(&mut self, entries: Vec<T>) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER accept_entries_leader: entries={:?}",
            self.pid,
            entries
                .iter()
                .map(|e| (e.key(), e.value().idx, &e.value().data))
                .collect::<Vec<_>>()
        );
        // EC-aware batch accept: encode each entry, store only leader's fragment, send correct fragment to each follower
        let total_shards = self.peers.len() + 1;
        let mut my_entries = Vec::with_capacity(entries.len());
        let mut all_entries: Vec<(String, OperationType, Vec<EntryFragment>)> =
            Vec::with_capacity(entries.len());

        for (_i, entry) in entries.iter().enumerate() {
            let key = entry.key().to_string();
            let op = entry.operation().clone();
            let fragments = self
                .ec_service
                .encode(&entry.value().data)
                .expect("EC encode failed");
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "[TRACE][LEADER {}] accept_entries_leader: entry {} key={} fragments={:?}",
                self.pid,
                _i,
                key,
                fragments
                    .iter()
                    .map(|f| (f.idx, &f.data))
                    .collect::<Vec<_>>()
            );
            let my_idx = ECService::fragment_index_for_node(self.pid as usize, total_shards);
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "[TRACE][LEADER {}] accept_entries_leader: entry {} my_idx={} (pid={})",
                self.pid,
                _i,
                my_idx,
                self.pid
            );
            let my_entry = T::from_parts(key.clone(), fragments[my_idx].clone(), op.clone());

            my_entries.push(my_entry);
            all_entries.push((key, op, fragments));
        }

        let accepted_metadata = self
            .internal_storage
            .append_entries_with_batching(my_entries)
            .expect(WRITE_ERROR_MSG);

        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide_batch(&all_entries);
        }
    }

    /// EC-aware: send only the correct fragment to each follower for a single entry
    fn send_acceptdecide(&mut self, key: String, op: OperationType, fragments: Vec<EntryFragment>) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER send_acceptdecide: key={}, op={:?}, fragments={:?}",
            self.pid,
            key,
            op,
            fragments
                .iter()
                .map(|f| (f.idx, &f.data))
                .collect::<Vec<_>>()
        );
        let decided_idx = self.internal_storage.get_decided_idx();
        let total_shards = self.peers.len() + 1;
        for &pid in self.peers.iter() {
            let frag_idx = ECService::fragment_index_for_node(pid as usize, total_shards);
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "[TRACE][LEADER {}] send_acceptdecide: to pid={} frag_idx={} fragment={:?}",
                self.pid,
                pid,
                frag_idx,
                fragments[frag_idx]
            );
            let entry = T::from_parts(key.clone(), fragments[frag_idx].clone(), op.clone());
            let acc_dec = AcceptDecide {
                n: self.leader_state.n_leader,
                seq_num: self.leader_state.next_seq_num(pid),
                entries: vec![entry],
                decided_idx,
            };
            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: pid,
                msg: PaxosMsg::AcceptDecide(acc_dec),
            }));
        }
    }

    /// EC-aware: send only the correct fragment to each follower for a batch of entries
    fn send_acceptdecide_batch(
        &mut self,
        all_entries: &Vec<(String, OperationType, Vec<EntryFragment>)>,
    ) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER send_acceptdecide_batch: all_entries.len={}",
            self.pid,
            all_entries.len()
        );
        let decided_idx = self.internal_storage.get_decided_idx();
        let total_shards = self.peers.len() + 1;
        for &pid in self.peers.iter() {
            let mut entries = Vec::with_capacity(all_entries.len());
            for (_i, (key, op, fragments)) in all_entries.iter().enumerate() {
                let frag_idx = ECService::fragment_index_for_node(pid as usize, total_shards);
                #[cfg(feature = "logging")]
                info!(
                    self.logger,
                    "[TRACE][LEADER {}] send_acceptdecide_batch: to pid={} entry {} frag_idx={} fragment={:?}",
                    self.pid,
                    pid,
                    _i,
                    frag_idx,
                    fragments[frag_idx]
                );
                entries.push(T::from_parts(
                    key.clone(),
                    fragments[frag_idx].clone(),
                    op.clone(),
                ));
            }
            if !entries.is_empty() {
                let acc_dec = AcceptDecide {
                    n: self.leader_state.n_leader,
                    seq_num: self.leader_state.next_seq_num(pid),
                    entries,
                    decided_idx,
                };
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: pid,
                    msg: PaxosMsg::AcceptDecide(acc_dec),
                }));
            }
        }
    }

    /// EC-aware log sync: send only the correct fragments for the requested log range
    fn send_accsync(&mut self, to: NodeId) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER send_accsync to {}", self.pid, to
        );
        // Follower can have valid accepted entries depending on which leader they were previously following
        let current_n = self.leader_state.n_leader;
        let PromiseMetaData {
            n_accepted: prev_round_max_promise_n,
            accepted_idx: prev_round_max_accepted_idx,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n_accepted: followers_promise_n,
            accepted_idx: followers_accepted_idx,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let followers_decided_idx = self
            .leader_state
            .get_decided_idx(*pid)
            .expect("Received PromiseMetaData but not found in ld");
        let followers_valid_entries_idx = if *followers_promise_n == current_n {
            *followers_accepted_idx
        } else if *followers_promise_n == *prev_round_max_promise_n {
            *prev_round_max_accepted_idx.min(followers_accepted_idx)
        } else {
            followers_decided_idx
        };
        // Create EC-aware LogSync: only the correct fragment for 'to' in the suffix
        let mut log_sync = self.create_log_sync(followers_valid_entries_idx, followers_decided_idx);
        let total_shards = self.peers.len() + 1;
        // For each entry in the suffix, replace with only the correct fragment for 'to'
        for entry in log_sync.suffix.iter_mut() {
            let fragments = self
                .ec_service
                .encode(&entry.value().data)
                .expect("EC encode failed");
            let frag_idx = ECService::fragment_index_for_node(to as usize, total_shards);
            let new_entry = T::from_parts(
                entry.key().to_string(),
                fragments[frag_idx].clone(),
                entry.operation().clone(),
            );
            *entry = new_entry;
        }
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.get_decided_idx(),
            log_sync,
            // #[cfg(feature = "unicache")]
            // unicache: self.internal_storage.get_unicache(),
        };
        let msg = Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptSync(acc_sync),
        });
        self.outgoing.push(msg);
    }

    pub(crate) fn accept_stopsign_leader(&mut self, ss: StopSign<ClusterConfigEC>) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER accept_stopsign_leader: stopsign={:?}", self.pid, ss
        );
        let accepted_metadata = self
            .internal_storage
            .append_stopsign(ss.clone())
            .expect(WRITE_ERROR_MSG);
        if let Some(_metadata) = accepted_metadata {
            // Encode the stopsign as a value and send only the correct fragment to each follower
            let value_bytes = bincode::serialize(&ss).expect("StopSign must be serializable");
            let fragments = self
                .ec_service
                .encode(&value_bytes)
                .expect("EC encode failed");
            let key = "stopsign".to_string();
            // Operation is null because it is a control message
            let op = OperationType::NULL;
            self.send_acceptdecide(key, op, fragments);
        }
        let accepted_idx = self.internal_storage.get_accepted_idx();
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accept_stopsign(pid, ss.clone(), false);
        }
    }

    fn send_accept_stopsign(&mut self, to: NodeId, ss: StopSign<ClusterConfigEC>, resend: bool) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER send_accept_stopsign(to={}, resend={})", self.pid, to, resend
        );
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            seq_num,
            n: self.leader_state.n_leader,
            ss,
        });
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: acc_ss,
        }));
    }

    pub(crate) fn send_decide(&mut self, to: NodeId, decided_idx: usize, resend: bool) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER send_decide(to={}, decided_idx={}, resend={})",
            self.pid,
            to,
            decided_idx,
            resend
        );
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let d = Decide {
            n: self.leader_state.n_leader,
            seq_num,
            decided_idx,
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Decide(d),
        }));
    }

    fn handle_majority_promises(&mut self) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER handle_majority_promises", self.pid
        );
        let max_promise_sync = self.leader_state.take_max_promise_sync();
        let decided_idx = self.leader_state.get_max_decided_idx();
        let mut new_accepted_idx = self
            .internal_storage
            .sync_log(self.leader_state.n_leader, decided_idx, max_promise_sync)
            .expect(WRITE_ERROR_MSG);
        if !self.accepted_reconfiguration() {
            if !self.buffered_proposals.is_empty() {
                let entries = std::mem::take(&mut self.buffered_proposals);
                new_accepted_idx = self
                    .internal_storage
                    .append_entries_without_batching(entries)
                    .expect(WRITE_ERROR_MSG);
            }
            if let Some(ss) = self.buffered_stopsign.take() {
                self.internal_storage
                    .append_stopsign(ss)
                    .expect(WRITE_ERROR_MSG);
                new_accepted_idx = self.internal_storage.get_accepted_idx();
            }
        }
        self.state = (RoleEC::Leader, PhaseEC::Accept);
        self.leader_state
            .set_accepted_idx(self.pid, new_accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_prepare(
        &mut self,
        prom: Promise<T, ClusterConfigEC>,
        from: NodeId,
    ) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER handle_promise_prepare(from={})", self.pid, from
        );
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises();
            }
        }
    }

    pub(crate) fn handle_promise_accept(
        &mut self,
        prom: Promise<T, ClusterConfigEC>,
        from: NodeId,
    ) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER handle_promise_accept(from={})", self.pid, from
        );
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            info!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from);
        }
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted, from: NodeId) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER handle_accepted(from={})", self.pid, from
        );
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}, accepted: {:?}",
            from,
            accepted.accepted_idx,
            self.internal_storage.get_decided_idx(),
            self.leader_state.accepted_indexes
        );
        if accepted.n == self.leader_state.n_leader
            && self.state == (RoleEC::Leader, PhaseEC::Accept)
        {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            if accepted.accepted_idx > self.internal_storage.get_decided_idx()
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                let decided_idx = accepted.accepted_idx;
                self.internal_storage
                    .set_decided_idx(decided_idx)
                    .expect(WRITE_ERROR_MSG);
                for pid in self.leader_state.get_promised_followers() {
                    let latest_accdec = self.get_latest_accdec_message(pid);
                    match latest_accdec {
                        Some(accdec) => accdec.decided_idx = decided_idx,
                        None => self.send_decide(pid, decided_idx, false),
                    }
                }
            }
        }
    }

    fn get_latest_accdec_message(&mut self, to: NodeId) -> Option<&mut AcceptDecide<T>> {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER get_latest_accdec_message(to={})", self.pid, to
        );
        if let Some((bal, outgoing_idx)) = self.leader_state.get_latest_accept_meta(to) {
            if bal == self.leader_state.n_leader {
                if let Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::AcceptDecide(accdec),
                    ..
                }) = self.outgoing.get_mut(outgoing_idx).unwrap()
                {
                    return Some(accdec);
                } else {
                    #[cfg(feature = "logging")]
                    info!(self.logger, "Cached idx is not an AcceptedDecide!");
                }
            }
        }
        None
    }

    pub(crate) fn handle_notaccepted(&mut self, not_acc: NotAccepted, from: NodeId) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER handle_notaccepted(from={})", self.pid, from
        );
        if self.state.0 == RoleEC::Leader && self.leader_state.n_leader < not_acc.n {
            self.leader_state.lost_promise(from);
        }
    }

    pub(crate) fn resend_messages_leader(&mut self) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER resend_messages_leader", self.pid
        );
        match self.state.1 {
            PhaseEC::Prepare => {
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            PhaseEC::Accept => {
                // Resend AcceptStopSign or StopSign's decide
                if let Some(ss) = self.internal_storage.get_stopsign() {
                    let decided_idx = self.internal_storage.get_decided_idx();
                    for follower in self.leader_state.get_promised_followers() {
                        if self.internal_storage.stopsign_is_decided() {
                            self.send_decide(follower, decided_idx, true);
                        } else if self.leader_state.get_accepted_idx(follower)
                            != self.internal_storage.get_accepted_idx()
                        {
                            self.send_accept_stopsign(follower, ss.clone(), true);
                        }
                    }
                }
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            PhaseEC::Recover => (),
            PhaseEC::None => (),
        }
    }

    // EC-aware: reconstruct (key, op, fragments) for each entry in the batch
    pub(crate) fn flush_batch_leader(&mut self) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[TRACE][LEADER {}] ENTER flush_batch_leader", self.pid
        );
        let accepted_metadata = self
            .internal_storage
            .flush_batch_and_get_entries()
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);

            let mut all_fragments = Vec::with_capacity(metadata.entries.len());

            for entry in metadata.entries.iter() {
                let key = entry.key().to_string();
                let op = entry.operation().clone();
                let fragments = self
                    .ec_service
                    .encode(&entry.value().data)
                    .expect("EC encode failed");

                all_fragments.push((key, op, fragments));
            }
            self.send_acceptdecide_batch(&all_fragments);
        }
    }
}
