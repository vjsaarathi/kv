package dev.kv.storage;

import dev.kv.raft.LogEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The deterministic state machine driven by committed log entries.
 *
 * <p>Raft replicates commands, rather than map mutations. Every node applies the same committed
 * command sequence here, so their maps converge. Callers must serialize access; {@link RaftNode}
 * does that with its state lock.
 */
public final class StateMachine {
  private final Map<String, String> values = new HashMap<>();

  /** Applies one already-committed entry exactly once. */
  public void apply(LogEntry entry) {
    if ("PUT".equals(entry.op())) values.put(entry.key(), entry.value());
    else if ("DELETE".equals(entry.op())) values.remove(entry.key());
    else throw new IllegalArgumentException("unknown operation " + entry.op());
  }

  /** Returns an absent optional for a key that has never been put or was deleted. */
  public Optional<String> get(String key) {
    return Optional.ofNullable(values.get(key));
  }

  /** Returns an immutable diagnostic snapshot rather than exposing the mutable map. */
  public Map<String, String> copy() {
    return Map.copyOf(values);
  }
}
