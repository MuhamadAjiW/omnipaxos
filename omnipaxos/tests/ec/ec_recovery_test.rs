use crate::ec::utils::{
    create_proposals, verification::verify_log, StorageTypeEC, TestConfigEC, TestECEntry,
    TestSystemEC,
};
use kompact::prelude::{promise, Ask, FutureCollection, KFuture};
use omnipaxos::{
    util::{LogEntry, NodeId},
    ClusterConfigEC,
};
use serial_test::serial;
use std::{thread, time::Duration};

const SLEEP_TIMEOUT: Duration = Duration::from_secs(1);

#[test]
#[serial]
#[ignore]
fn ec_leader_fail_follower_propose_test() {
    let cfg = TestConfigEC::load("recovery_test").expect("Test config loaded");
    let mut sys = TestSystemEC::with(cfg);
    sys.start_all_nodes();

    let proposals: Vec<TestECEntry> = (1..=cfg.num_proposals).map(TestECEntry::dummy).collect();
    let initial_proposals = proposals[0..(cfg.num_proposals / 2) as usize].to_vec();
    sys.make_proposals(1, initial_proposals, cfg.wait_timeout);
    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let follower = (1..=cfg.num_nodes as NodeId)
        .find(|x| *x != leader)
        .expect("No followers found!");

    kill_and_recover_node(&mut sys, &cfg, leader);
    check_last_proposals(follower, leader, &sys, &cfg);

    thread::sleep(SLEEP_TIMEOUT);

    let recovery_px = sys
        .nodes
        .get(&leader)
        .expect("No SequencePaxos component found");
    let read_log: Vec<LogEntry<TestECEntry, ClusterConfigEC>> =
        recovery_px.on_definition(|x| x.read_decided_log());

    verify_log(read_log, proposals);

    println!("Pass leader_fail_follower_propose!");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
#[ignore]
fn ec_leader_fail_leader_propose_test() {
    let cfg = TestConfigEC::load("recovery_test").expect("Test config loaded");
    let mut sys = TestSystemEC::with(cfg);
    sys.start_all_nodes();

    let proposals: Vec<TestECEntry> = (1..=cfg.num_proposals).map(TestECEntry::dummy).collect();
    let initial_proposals = proposals[0..(cfg.num_proposals / 2) as usize].to_vec();
    sys.make_proposals(1, initial_proposals, cfg.wait_timeout);
    let leader = sys.get_elected_leader(1, cfg.wait_timeout);

    kill_and_recover_node(&mut sys, &cfg, leader);
    check_last_proposals(leader, leader, &sys, &cfg);

    thread::sleep(SLEEP_TIMEOUT);

    let recovery_px = sys
        .nodes
        .get(&leader)
        .expect("No SequencePaxos component found");
    let read_log: Vec<LogEntry<TestECEntry, ClusterConfigEC>> =
        recovery_px.on_definition(|x| x.read_decided_log());

    verify_log(read_log, proposals);

    println!("Pass leader_fail_leader_propose!");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
#[ignore]
fn ec_follower_fail_leader_propose_test() {
    let cfg = TestConfigEC::load("recovery_test").expect("Test config loaded");
    let mut sys = TestSystemEC::with(cfg);
    sys.start_all_nodes();

    let proposals: Vec<TestECEntry> = (1..=cfg.num_proposals).map(TestECEntry::dummy).collect();
    let initial_proposals = proposals[0..(cfg.num_proposals / 2) as usize].to_vec();
    sys.make_proposals(1, initial_proposals, cfg.wait_timeout);
    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let follower = (1..=cfg.num_nodes as NodeId)
        .find(|x| *x != leader)
        .expect("No followers found!");

    kill_and_recover_node(&mut sys, &cfg, follower);
    check_last_proposals(leader, follower, &sys, &cfg);

    thread::sleep(SLEEP_TIMEOUT);

    let recovery_px = sys
        .nodes
        .get(&leader)
        .expect("No SequencePaxos component found");
    let read_log: Vec<LogEntry<TestECEntry, ClusterConfigEC>> =
        recovery_px.on_definition(|x| x.read_decided_log());

    verify_log(read_log, proposals);

    println!("Pass follower_fail_leader_propose");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

#[test]
#[serial]
#[ignore]
fn ec_follower_fail_follower_propose_test() {
    let cfg = TestConfigEC::load("recovery_test").expect("Test config loaded");
    let mut sys = TestSystemEC::with(cfg);
    sys.start_all_nodes();

    let proposals: Vec<TestECEntry> = (1..=cfg.num_proposals).map(TestECEntry::dummy).collect();
    let initial_proposals = proposals[0..(cfg.num_proposals / 2) as usize].to_vec();
    sys.make_proposals(1, initial_proposals, cfg.wait_timeout);
    let leader = sys.get_elected_leader(1, cfg.wait_timeout);
    let follower = (1..=cfg.num_nodes as NodeId)
        .find(|x| *x != leader)
        .expect("No followers found!");

    kill_and_recover_node(&mut sys, &cfg, follower);
    check_last_proposals(follower, follower, &sys, &cfg);

    thread::sleep(SLEEP_TIMEOUT);

    let recovery_px = sys
        .nodes
        .get(&leader)
        .expect("No SequencePaxos component found");
    let read_log: Vec<LogEntry<TestECEntry, ClusterConfigEC>> =
        recovery_px.on_definition(|x| x.read_decided_log());

    verify_log(read_log, proposals);

    println!("Pass follower_fail_follower_propose");

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}

/// Propose and check that the last proposals are decided by the
/// recovered node. The recovered node can also be the proposer
fn check_last_proposals(proposer: NodeId, recover: NodeId, sys: &TestSystemEC, cfg: &TestConfigEC) {
    let proposer_px = sys
        .nodes
        .get(&proposer)
        .expect("No SequencePaxos component found");
    let recover_px = sys
        .nodes
        .get(&recover)
        .expect("No SequencePaxos component found");

    let proposals = create_proposals((cfg.num_proposals / 2) + 1, cfg.num_proposals);
    let futures: Vec<KFuture<()>> = proposals
        .iter()
        .map(|v| {
            let (kprom, kfuture) = promise::<()>();
            recover_px.on_definition(|x| {
                x.insert_decided_future(Ask::new(kprom, v.clone()));
            });
            kfuture
        })
        .collect();

    for v in proposals {
        proposer_px.on_definition(|x| {
            x.paxos.append(v).expect("Failed to append");
        });
    }

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }
}

/// Kill and recover node given its 'pid' after some time.
pub fn kill_and_recover_node(sys: &mut TestSystemEC, cfg: &TestConfigEC, pid: NodeId) {
    sys.kill_node(pid);
    thread::sleep(SLEEP_TIMEOUT);

    let storage_path = sys.temp_dir_path.clone();
    let storage: StorageTypeEC<TestECEntry> =
        StorageTypeEC::with(cfg.storage_type, &format!("{storage_path}{pid}"));
    sys.create_node(pid, cfg, storage);
    sys.start_node(pid);
}
