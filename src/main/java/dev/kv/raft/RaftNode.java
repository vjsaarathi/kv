package dev.kv.raft;

import dev.kv.protocol.FrameCodec;
import dev.kv.protocol.Rpc;
import dev.kv.storage.PersistentStore;
import dev.kv.storage.StateMachine;
import java.io.*;
import java.net.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A complete Raft peer and key-value RPC endpoint.
 *
 * <p>All fields that describe Raft state are protected by {@link #lock}. The timer only decides
 * when to begin elections or send heartbeats; slow network requests run in the bounded worker pool
 * and never hold that lock. Index zero is an implicit log sentinel with term zero, so the first
 * actual entry has index one.
 *
 * <p>The durable state is {@code term}, {@code vote}, {@code commit}, and {@code log}. The role,
 * leader identity, applied index, and leader replication indexes are rebuilt after a restart. A
 * state-machine change occurs only when {@code applied} advances through {@code commit}.
 */
public final class RaftNode implements AutoCloseable {
  /** Raft role; only a leader accepts normal client reads and mutations. */
  public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
  }

  private final String id, bind;
  private final Map<String, String> peers;
  private final PersistentStore disk;
  private final ReentrantLock lock = new ReentrantLock();
  private final StateMachine machine = new StateMachine();
  private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService workers =
      new ThreadPoolExecutor(
          4,
          32,
          60,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(256),
          new ThreadPoolExecutor.CallerRunsPolicy());
  private final Random random = new Random();
  private final List<LogEntry> log;
  private long term, commit, applied;
  private String vote, leader;
  private Role role = Role.FOLLOWER;
  private long electionAt;
  private ServerSocket socket;
  private volatile boolean running;
  private Map<String, Long> next = new HashMap<>(), matched = new HashMap<>();

  /**
   * Restores durable state and prepares a stopped node. The supplied peers map must include this
   * node ID and is immutable for this node's lifetime.
   */
  public RaftNode(String id, String bind, Map<String, String> peers, Path dataDir)
      throws IOException {
    this.id = id;
    this.bind = bind;
    this.peers = Map.copyOf(peers);
    this.disk = new PersistentStore(dataDir);
    var loaded = disk.load();
    term = loaded.meta().term();
    vote = loaded.meta().votedFor();
    commit = loaded.meta().commitIndex();
    log = loaded.log();
    applyLocked();
    resetElectionLocked();
  }

  /**
   * Opens the listener, begins accepting one-request TCP connections, and starts the Raft timer.
   */
  public void start() throws IOException {
    InetSocketAddress a = parse(bind);
    socket = new ServerSocket();
    socket.bind(a);
    running = true;
    workers.submit(
        () -> {
          while (running)
            try {
              Socket s = socket.accept();
              workers.submit(() -> serve(s));
            } catch (IOException e) {
              if (running) e.printStackTrace();
            }
        });
    timer.scheduleAtFixedRate(this::tick, 30, 30, TimeUnit.MILLISECONDS);
  }

  /** Reads, dispatches, and responds to one frame; connection failure affects only this request. */
  private void serve(Socket s) {
    try (s) {
      s.setSoTimeout(1500);
      Rpc request = FrameCodec.read(s.getInputStream());
      if (request != null) FrameCodec.write(s.getOutputStream(), dispatch(request));
    } catch (IOException ignored) {
    }
  }

  /** Routes the envelope to its Raft or client handler. */
  private Rpc dispatch(Rpc q) {
    try {
      return switch (q.type()) {
        case "VOTE" -> vote(q);
        case "APPEND" -> append(q);
        case "CLIENT" -> client(q);
        default -> err("BAD_REQUEST", "unknown rpc");
      };
    } catch (Exception e) {
      return err("ERROR", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
    }
  }

  /**
   * Runs frequently: followers start an election after their deadline, while leaders broadcast
   * empty AppendEntries calls as heartbeats.
   */
  private void tick() {
    try {
      lock.lock();
      try {
        if (!running) return;
        if (role == Role.LEADER) {
        } else if (System.nanoTime() >= electionAt) {
          workers.submit(this::startElection);
          return;
        } else return;
      } finally {
        lock.unlock();
      }
      broadcast(null);
    } catch (Exception ignored) {
    }
  }

  /**
   * Becomes a candidate, durably records a self-vote, and counts peer votes. A response with a
   * higher term immediately invalidates this election.
   */
  private void startElection() {
    long electionTerm, lastIndex, lastTerm;
    lock.lock();
    try {
      if (!running || role == Role.LEADER || System.nanoTime() < electionAt) return;
      role = Role.CANDIDATE;
      term++;
      vote = id;
      leader = null;
      persistMeta();
      resetElectionLocked();
      electionTerm = term;
      lastIndex = log.size();
      lastTerm = termAt(lastIndex);
    } finally {
      lock.unlock();
    }
    List<CompletableFuture<Rpc>> calls = new ArrayList<>();
    for (var peer : peers.entrySet())
      if (!peer.getKey().equals(id))
        calls.add(
            CompletableFuture.supplyAsync(
                () ->
                    call(
                        peer.getValue(),
                        Rpc.of(
                            "VOTE",
                            Map.of(
                                "term",
                                electionTerm,
                                "candidate",
                                id,
                                "lastIndex",
                                lastIndex,
                                "lastTerm",
                                lastTerm))),
                workers));
    int votes = 1;
    for (var f : calls) {
      Rpc r = f.join();
      if (r != null) {
        stepDownIfHigher(num(r.body(), "term"));
        if (bool(r.body(), "granted")) votes++;
      }
    }
    lock.lock();
    try {
      if (role == Role.CANDIDATE && term == electionTerm && votes >= majority())
        becomeLeaderLocked();
    } finally {
      lock.unlock();
    }
  }

  /** Handles RequestVote: grants at most one vote per term to an up-to-date log. */
  private Rpc vote(Rpc q) {
    long theirTerm = num(q.body(), "term"),
        li = num(q.body(), "lastIndex"),
        lt = num(q.body(), "lastTerm");
    String candidate = str(q.body(), "candidate");
    lock.lock();
    try {
      if (theirTerm < term) return Rpc.of("VOTE_RESULT", Map.of("term", term, "granted", false));
      if (theirTerm > term) stepDownLocked(theirTerm);
      boolean upToDate = lt > termAt(log.size()) || (lt == termAt(log.size()) && li >= log.size());
      boolean granted = upToDate && (vote == null || vote.equals(candidate));
      if (granted) {
        vote = candidate;
        persistMeta();
        resetElectionLocked();
      }
      return Rpc.of("VOTE_RESULT", Map.of("term", term, "granted", granted));
    } finally {
      lock.unlock();
    }
  }

  /**
   * Handles AppendEntries and heartbeats. The previous index/term pair proves that the leader and
   * follower share a prefix. A mismatch rejects the RPC; a match permits replacing only the
   * follower's divergent suffix.
   */
  private Rpc append(Rpc q) throws IOException {
    long theirTerm = num(q.body(), "term"),
        prev = num(q.body(), "prevIndex"),
        prevTerm = num(q.body(), "prevTerm"),
        leaderCommit = num(q.body(), "leaderCommit");
    String source = str(q.body(), "leader");
    String context = str(q.body(), "readContext");
    List<LogEntry> entries =
        FrameCodec.JSON.convertValue(
            q.body().get("entries"),
            FrameCodec.JSON.getTypeFactory().constructCollectionType(List.class, LogEntry.class));
    lock.lock();
    try {
      if (theirTerm < term) return appendResult(false, 0, context);
      if (theirTerm > term || role != Role.FOLLOWER) stepDownLocked(theirTerm);
      leader = source;
      resetElectionLocked();
      if (prev > log.size() || termAt(prev) != prevTerm)
        return appendResult(false, Math.min(prev, log.size()), context);
      int from = (int) prev;
      boolean changed = false;
      for (int i = 0; i < entries.size(); i++) {
        int at = from + i;
        if (at < log.size() && !log.get(at).equals(entries.get(i))) {
          while (log.size() > at) log.remove(log.size() - 1);
          changed = true;
        }
        if (at >= log.size()) {
          log.add(entries.get(i));
          changed = true;
        }
      }
      if (changed) disk.saveLog(log);
      if (leaderCommit > commit) {
        commit = Math.min(leaderCommit, log.size());
        persistMeta();
        applyLocked();
      }
      return appendResult(true, log.size(), context);
    } finally {
      lock.unlock();
    }
  }

  /** Builds the shared AppendEntries response, echoing a read-barrier context when present. */
  private Rpc appendResult(boolean success, long match, String context) {
    Map<String, Object> b = new HashMap<>();
    b.put("term", term);
    b.put("success", success);
    b.put("match", match);
    b.put("readContext", context);
    return Rpc.of("APPEND_RESULT", b);
  }

  /** Handles public operations and redirects everything except STATUS away from followers. */
  private Rpc client(Rpc q) throws IOException {
    String op = str(q.body(), "op");
    if ("STATUS".equals(op)) return status();
    lock.lock();
    try {
      if (role != Role.LEADER) return notLeader();
    } finally {
      lock.unlock();
    }
    return switch (op) {
      case "PUT", "DELETE" -> mutate(op, str(q.body(), "key"), str(q.body(), "value"));
      case "GET" -> get(str(q.body(), "key"));
      default -> err("BAD_REQUEST", "unknown command");
    };
  }

  /**
   * Appends a mutation locally, replicates it to a majority, then advances the commit index and
   * applies it. An uncommitted entry remains in the log after a timeout; a later leader may still
   * commit it according to normal Raft rules.
   */
  private Rpc mutate(String op, String key, String value) throws IOException {
    long index, myTerm;
    lock.lock();
    try {
      if (role != Role.LEADER) return notLeader();
      LogEntry entry = new LogEntry(term, op, key, value);
      log.add(entry);
      disk.appendLog(entry);
      index = log.size();
      myTerm = term;
    } finally {
      lock.unlock();
    }
    if (!quorum(null, index, Duration.ofMillis(1300)))
      return err("UNAVAILABLE", "could not reach quorum");
    lock.lock();
    try {
      if (role != Role.LEADER || term != myTerm) return notLeader();
      commit = index;
      persistMeta();
      applyLocked();
    } finally {
      lock.unlock();
    }
    broadcast(null);
    return Rpc.of("OK", Map.of("index", index));
  }

  /**
   * Performs a ReadIndex-style barrier before reading local applied state. The quorum round proves
   * this leader still holds authority in its term.
   */
  private Rpc get(String key) {
    String context = UUID.randomUUID().toString();
    if (!quorum(context, 0, Duration.ofMillis(1100)))
      return err("UNAVAILABLE", "could not confirm leadership");
    lock.lock();
    try {
      if (role != Role.LEADER) return notLeader();
      var value = machine.get(key);
      Map<String, Object> b = new HashMap<>();
      b.put("found", value.isPresent());
      b.put("value", value.orElse(null));
      return Rpc.of("VALUE", b);
    } finally {
      lock.unlock();
    }
  }

  /** Sends replication/heartbeat calls concurrently and returns once a majority acknowledges. */
  private boolean quorum(String context, long needed, Duration timeout) {
    List<CompletableFuture<Boolean>> fs = new ArrayList<>();
    for (var p : peers.entrySet())
      if (!p.getKey().equals(id))
        fs.add(
            CompletableFuture.supplyAsync(
                () -> replicate(p.getKey(), p.getValue(), context, needed), workers));
    int acks = 1;
    long end = System.nanoTime() + timeout.toNanos();
    for (var f : fs) {
      long left = end - System.nanoTime();
      if (left <= 0) break;
      try {
        if (f.get(left, TimeUnit.NANOSECONDS)) acks++;
      } catch (Exception ignored) {
      }
    }
    return acks >= majority();
  }

  /** Schedules best-effort replication to every follower; heartbeat callers pass no context. */
  private void broadcast(String context) {
    for (var p : peers.entrySet())
      if (!p.getKey().equals(id))
        workers.submit(() -> replicate(p.getKey(), p.getValue(), context, 0));
  }

  /**
   * Advances one follower. On a prefix rejection it decrements nextIndex and retries, eventually
   * locating the common prefix or exhausting the attempt limit.
   */
  private boolean replicate(String peer, String address, String context, long needed) {
    for (int attempts = 0; attempts < 8; attempts++) {
      long ni, myTerm, prev;
      List<LogEntry> entries;
      long ci;
      lock.lock();
      try {
        if (role != Role.LEADER) return false;
        ni = next.getOrDefault(peer, (long) log.size() + 1);
        prev = ni - 1;
        myTerm = term;
        ci = commit;
        entries = new ArrayList<>(log.subList((int) prev, log.size()));
      } finally {
        lock.unlock();
      }
      Map<String, Object> b = new HashMap<>();
      b.put("term", myTerm);
      b.put("leader", id);
      b.put("prevIndex", prev);
      b.put("prevTerm", termAtSafe(prev));
      b.put("entries", entries);
      b.put("leaderCommit", ci);
      b.put("readContext", context);
      Rpc r = call(address, Rpc.of("APPEND", b));
      if (r == null) return false;
      long rt = num(r.body(), "term");
      if (stepDownIfHigher(rt)) return false;
      if (bool(r.body(), "success")) {
        long match = num(r.body(), "match");
        lock.lock();
        try {
          if (role == Role.LEADER && term == myTerm) {
            matched.put(peer, match);
            next.put(peer, match + 1);
          }
        } finally {
          lock.unlock();
        }
        return match >= needed;
      }
      lock.lock();
      try {
        next.put(peer, Math.max(1, ni - 1));
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  /** Reads a log term under the state lock for request construction. */
  private long termAtSafe(long i) {
    lock.lock();
    try {
      return termAt(i);
    } finally {
      lock.unlock();
    }
  }

  /** Returns a non-mutating view of local Raft state; safe to call on any role. */
  private Rpc status() {
    lock.lock();
    try {
      Map<String, Object> b = new HashMap<>();
      b.put("id", id);
      b.put("role", role.name());
      b.put("term", term);
      b.put("leader", leader);
      b.put("commitIndex", commit);
      b.put("lastIndex", log.size());
      return Rpc.of("STATUS", b);
    } finally {
      lock.unlock();
    }
  }

  /** Returns the configured address of the leader, allowing a client to redirect itself. */
  private Rpc notLeader() {
    Map<String, Object> b = new HashMap<>();
    b.put("leader", leader == null ? null : peers.get(leader));
    return Rpc.of("NOT_LEADER", b);
  }

  private Rpc err(String code, String message) {
    return Rpc.of(code, Map.of("message", message));
  }

  /**
   * Initializes per-follower indexes. nextIndex starts one position beyond the leader's last entry.
   */
  private void becomeLeaderLocked() {
    role = Role.LEADER;
    leader = id;
    next = new HashMap<>();
    matched = new HashMap<>();
    for (String p : peers.keySet())
      if (!p.equals(id)) {
        next.put(p, (long) log.size() + 1);
        matched.put(p, 0L);
      }
  }

  /** Observes a higher term from a remote peer and converts this node to a follower. */
  private boolean stepDownIfHigher(long t) {
    lock.lock();
    try {
      if (t > term) {
        stepDownLocked(t);
        return true;
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  /** Clears a vote only when term increases, then resets follower election timing. */
  private void stepDownLocked(long newTerm) {
    if (newTerm > term) {
      term = newTerm;
      vote = null;
      persistMeta();
    }
    role = Role.FOLLOWER;
    leader = null;
    resetElectionLocked();
  }

  /** Randomizes elections so multiple followers are unlikely to become candidates together. */
  private void resetElectionLocked() {
    electionAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(450 + random.nextInt(300));
  }

  /** Persists all durable scalar state as one metadata update. */
  private void persistMeta() {
    try {
      disk.saveMeta(new PersistentStore.Meta(term, vote, commit));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Applies the not-yet-applied committed range; log index 1 maps to list offset 0. */
  private void applyLocked() {
    while (applied < commit) {
      machine.apply(log.get((int) applied));
      applied++;
    }
  }

  private long termAt(long index) {
    return index == 0 ? 0 : log.get((int) index - 1).term();
  }

  private int majority() {
    return peers.size() / 2 + 1;
  }

  private static long num(Map<String, Object> b, String k) {
    Object v = b.get(k);
    return v instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(v));
  }

  private static boolean bool(Map<String, Object> b, String k) {
    Object v = b.get(k);
    return v instanceof Boolean x ? x : Boolean.parseBoolean(String.valueOf(v));
  }

  private static String str(Map<String, Object> b, String k) {
    Object v = b.get(k);
    return v == null ? null : String.valueOf(v);
  }

  /** Sends one request over a short-lived TCP connection; unavailable peers return {@code null}. */
  public static Rpc call(String address, Rpc q) {
    try (Socket s = new Socket()) {
      s.connect(parse(address), 500);
      s.setSoTimeout(1200);
      FrameCodec.write(s.getOutputStream(), q);
      return FrameCodec.read(s.getInputStream());
    } catch (IOException e) {
      return null;
    }
  }

  /** Parses the static {@code host:port} address format used in peer configuration. */
  static InetSocketAddress parse(String address) {
    int colon = address.lastIndexOf(':');
    if (colon < 1) throw new IllegalArgumentException("address must be host:port");
    return new InetSocketAddress(
        address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)));
  }

  /** Returns an immutable diagnostic snapshot used by tests and local inspection. */
  public Map<String, Object> debugState() {
    lock.lock();
    try {
      return Map.of("term", term, "commit", commit, "role", role.name(), "data", machine.copy());
    } finally {
      lock.unlock();
    }
  }

  /** Stops new I/O, closes the listening socket, and interrupts timer/worker tasks. */
  @Override
  public void close() {
    running = false;
    try {
      if (socket != null) socket.close();
    } catch (IOException ignored) {
    }
    timer.shutdownNow();
    workers.shutdownNow();
  }
}
