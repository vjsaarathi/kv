package dev.kv.cli;

import dev.kv.protocol.Rpc;
import dev.kv.raft.RaftNode;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Command-line client for the public key-value operations.
 *
 * <p>The client may contact any configured seed. Mutating and read requests sent to a follower
 * receive {@code NOT_LEADER}; the returned address is used for one retry. Retrying only once
 * prevents an endless loop during an election or partition and keeps a temporary lack of quorum
 * visible to the caller.
 */
public final class ClientMain {
  /** Converts one CLI command into a {@code CLIENT} RPC and prints its response. */
  public static void main(String[] args) {
    Map<String, String> options = NodeMain.args(sliceOptions(args));
    String seed = NodeMain.require(options, "seed");
    int commandAt = options.size() * 2;
    if (args.length <= commandAt)
      throw new IllegalArgumentException("command required: put|get|delete|status");
    String command = args[commandAt].toUpperCase();
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("op", command);
    if ("PUT".equals(command)) {
      need(args, commandAt, 3);
      body.put("key", args[commandAt + 1]);
      body.put("value", args[commandAt + 2]);
    } else if ("GET".equals(command) || "DELETE".equals(command)) {
      need(args, commandAt, 2);
      body.put("key", args[commandAt + 1]);
    } else if (!"STATUS".equals(command)) throw new IllegalArgumentException("unknown command");
    Rpc response = RaftNode.call(seed, Rpc.of("CLIENT", body));
    if (response != null
        && "NOT_LEADER".equals(response.type())
        && response.body().get("leader") != null)
      response =
          RaftNode.call(String.valueOf(response.body().get("leader")), Rpc.of("CLIENT", body));
    if (response == null) {
      System.err.println("UNAVAILABLE");
      System.exit(2);
    } else System.out.println(response.type() + " " + response.body());
  }

  /** Extracts the leading option pairs, leaving the operation and its arguments intact. */
  private static String[] sliceOptions(String[] a) {
    int i = 0;
    while (i < a.length && a[i].startsWith("--")) {
      if (i + 1 >= a.length) throw new IllegalArgumentException("option value missing");
      i += 2;
    }
    String[] r = new String[i];
    System.arraycopy(a, 0, r, 0, i);
    return r;
  }

  /** Checks that a command supplies its required positional arguments. */
  private static void need(String[] a, int at, int n) {
    if (a.length < at + n) throw new IllegalArgumentException("not enough command arguments");
  }
}
