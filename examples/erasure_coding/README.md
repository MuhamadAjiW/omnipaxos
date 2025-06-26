# Erasure Coding Example

This example shows how to build a erasure coding key-value store system using OmniPaxos. Each server is executed by one tokio thread and they communicate using tokio's mpsc channels.

- [ec.rs](/examples/ec/src/ec.rs) defines the `ErasureCoding` and `ECSnapshot` structs that will be stored in the log of OmniPaxos.
- [server.rs](/examples/ec/src/server.rs) implements the logic for an OmniPaxos server (i.e., a replica in our erasure coding system) and showcases how to send/receive messages and trigger the necessary timers.
- The [main](/examples/ec/src/main.rs) program spawns the OmniPaxos servers and shows how to append and read entries from the replicated log via different servers. We also show that a new leader will be elected if one of the servers fail.