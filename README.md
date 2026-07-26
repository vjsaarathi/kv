# Raft KV

A Java key-value store replicated by the Raft consensus algorithm. Each node accepts TCP connections, elects one leader, replicates ordered changes through the Raft log, and applies committed changes to a local map.

## Concepts and data flow

Every node has the same fixed membership list. At any time it is a `FOLLOWER`, `CANDIDATE`, or `LEADER`.

1. A follower that has not heard from a leader for a random 450–750 ms timeout starts an election.
2. It increments its durable term, votes for itself, and sends `VOTE` requests to its peers.
3. A candidate that receives a majority becomes leader. The leader sends `APPEND` heartbeats every 30 ms.
4. A client mutation is appended to the leader's durable log. The leader sends it to followers and commits it only after a majority acknowledges the entry.
5. Committed entries are applied in index order to the `StateMachine`. A `PUT` inserts/replaces a value and `DELETE` removes one.
6. A `GET` uses a read barrier: the leader sends a heartbeat with a unique read context, waits for a majority in its current term, then reads its applied state.

The quorum size is `floor(number of nodes / 2) + 1`. Any two quorums overlap in at least one node, which prevents conflicting entries at one committed log index.

## Protocol

Each TCP connection carries exactly one request and one response. A frame is:

```text
4-byte unsigned, big-endian JSON byte length | UTF-8 JSON payload
```

Frames larger than 1 MiB are rejected. The JSON `Rpc` envelope contains `type` and `body` fields. The message types are `VOTE`, `VOTE_RESULT`, `APPEND`, `APPEND_RESULT`, and `CLIENT`.

## Persistent data

Each node data directory contains:

- `meta.json` — current term, the node voted for in that term, and committed log index. It is atomically replaced.
- `raft-log.json` — length-framed `LogEntry` records. Normal changes append one record; conflict repair atomically rewrites the surviving prefix.

On startup, the node loads those files and replays entries through the persisted commit index. The log is not compacted in this version.

## Run a cluster

Build and run tests:

```sh
mvn test
```

Start these three commands in separate terminals:

```sh
mvn -q exec:java -Dexec.mainClass=dev.kv.cli.NodeMain -Dexec.args='--id n1 --bind 127.0.0.1:9001 --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data-dir /tmp/raft-kv/n1'
mvn -q exec:java -Dexec.mainClass=dev.kv.cli.NodeMain -Dexec.args='--id n2 --bind 127.0.0.1:9002 --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data-dir /tmp/raft-kv/n2'
mvn -q exec:java -Dexec.mainClass=dev.kv.cli.NodeMain -Dexec.args='--id n3 --bind 127.0.0.1:9003 --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data-dir /tmp/raft-kv/n3'
```

After election, send commands from a fourth terminal:

```sh
mvn -q exec:java -Dexec.mainClass=dev.kv.cli.ClientMain -Dexec.args='--seed 127.0.0.1:9001 status'
mvn -q exec:java -Dexec.mainClass=dev.kv.cli.ClientMain -Dexec.args='--seed 127.0.0.1:9001 put colour blue'
mvn -q exec:java -Dexec.mainClass=dev.kv.cli.ClientMain -Dexec.args='--seed 127.0.0.1:9002 get colour'
mvn -q exec:java -Dexec.mainClass=dev.kv.cli.ClientMain -Dexec.args='--seed 127.0.0.1:9003 delete colour'
```

The client follows one `NOT_LEADER` redirect. `STATUS` may be sent to any node. Stop the leader, wait for a new election, then retry a command against either remaining address to observe failover.

## Run with Docker Compose

Docker packages a process and its dependencies into an image. A container is one running copy of that image. Docker Compose reads `compose.yaml` and starts the three containers together on a private network.

The `node1`, `node2`, and `node3` service names are also DNS names within that private network. That is why the peer configuration uses `node1:9001` rather than a host IP address. Each named volume stores one node's Raft files outside its container, so stopping and recreating a container does not erase its state.

Start the cluster from this directory:

```sh
docker compose up --build -d
docker compose ps
```

`--build` creates the Java image when needed. `-d` leaves the three nodes running in the background. Wait about one second for leader election, then run client commands inside `node1`'s container:

Podman can run the same commands through its Docker-compatible CLI. The image names in `Dockerfile` include the full `docker.io/library/...` registry path, so Podman does not need to ask which registry to use. The message about `podman-compose` is informational: it identifies the Compose implementation handling the command.

```sh
docker compose exec node1 java -cp '/app/classes:/app/lib/*' dev.kv.cli.ClientMain --seed node1:9001 status
docker compose exec node1 java -cp '/app/classes:/app/lib/*' dev.kv.cli.ClientMain --seed node2:9002 put colour blue
docker compose exec node1 java -cp '/app/classes:/app/lib/*' dev.kv.cli.ClientMain --seed node3:9003 get colour
```

The client can use any node as its seed. If that node is not the leader, it follows the leader address included in the redirect response.

Useful Compose commands:

```sh
docker compose logs -f node1       # follow one node's output
docker compose stop node1          # stop one node without deleting its data
docker compose start node1         # restart the stopped node
docker compose down                # stop and remove containers; preserve volumes
docker compose down -v             # stop and remove containers and all node data
```

The final command deletes all persisted Raft logs and metadata. Use it only when a fresh cluster is required.

## Boundaries

This implementation does not provide TLS, authentication, snapshots, log compaction, dynamic membership, batching, transactions, compare-and-set, or TTLs. It runs on a trusted local network and emphasizes correct Raft state transitions over throughput.
