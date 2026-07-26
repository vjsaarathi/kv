package dev.kv.protocol;

import java.util.Map;

/**
 * The common envelope for every message sent on a TCP connection.
 *
 * <p>{@code type} selects the protocol operation, while {@code body} carries operation-specific
 * JSON-compatible values. Keeping one envelope lets a node use one listener for both client
 * requests and peer-to-peer Raft traffic.
 */
public record Rpc(String type, Map<String, Object> body) {
  /** Creates an envelope without exposing Jackson types to callers. */
  public static Rpc of(String type, Map<String, Object> body) {
    return new Rpc(type, body);
  }
}
