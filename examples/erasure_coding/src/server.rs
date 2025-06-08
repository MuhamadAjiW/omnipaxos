use crate::ec::ECKeyValue;
use omnipaxos::{messages::Message, util::NodeId, ClusterConfigEC};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    util::{OUTGOING_MESSAGE_PERIOD, TICK_PERIOD},
    OmniPaxosECKV,
};
use tokio::{sync::mpsc, time};

pub struct OmniPaxosServerEC {
    pub omni_paxos: Arc<Mutex<OmniPaxosECKV>>,
    pub incoming: mpsc::Receiver<Message<ECKeyValue, ClusterConfigEC>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<ECKeyValue, ClusterConfigEC>>>,
    pub message_buffer: Vec<Message<ECKeyValue, ClusterConfigEC>>,
}

impl OmniPaxosServerEC {
    async fn send_outgoing_msgs(&mut self) {
        self.omni_paxos
            .lock()
            .unwrap()
            .take_outgoing_messages(&mut self.message_buffer);
        for msg in self.message_buffer.drain(..) {
            let receiver = msg.get_receiver();
            let channel = self
                .outgoing
                .get_mut(&receiver)
                .expect("No channel for receiver");
            let _ = channel.send(msg).await;
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                biased;

                _ = tick_interval.tick() => { self.omni_paxos.lock().unwrap().tick(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(in_msg) = self.incoming.recv() => { self.omni_paxos.lock().unwrap().handle_incoming(in_msg); },
                else => { }
            }
        }
    }
}
