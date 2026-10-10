package dev.kv.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The deterministic state machine for our key-value store.
 */
public final class StateMachine {
  private final Map<String, String> values = new HashMap<>();

  /** Applies one mutation. */
  public void apply(String op, String key, String value) {
    if ("PUT".equals(op)) values.put(key, value);
    else if ("DELETE".equals(op)) values.remove(key);
    else throw new IllegalArgumentException("unknown operation " + op);
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
