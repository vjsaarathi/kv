package dev.kv.cli;

import dev.kv.raft.RaftNode;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Process entry point for one Raft node.
 *
 * <p>It translates command-line configuration into the fixed membership map required by {@link
 * RaftNode}, starts the listener, and installs a shutdown hook so sockets and executors are
 * released on {@code Ctrl-C}.
 */
public final class NodeMain {
  /** Starts a node from {@code --id}, {@code --bind}, {@code --peers}, and {@code --data-dir}. */
  public static void main(String[] args) throws Exception {
    Map<String, String> a = args(args);
    String id = require(a, "id"), bind = require(a, "bind"), peerText = require(a, "peers");
    Map<String, String> peers = new LinkedHashMap<>();
    for (String item : peerText.split(",")) {
      String[] p = item.split("=", 2);
      if (p.length != 2) throw new IllegalArgumentException("bad peer: " + item);
      peers.put(p[0], p[1]);
    }
    if (!peers.containsKey(id))
      throw new IllegalArgumentException("--peers must include this --id");
    RaftNode node = new RaftNode(id, bind, peers, Path.of(require(a, "data-dir")));
    node.start();
    Runtime.getRuntime().addShutdownHook(new Thread(node::close));
    System.out.println("node " + id + " listening on " + bind);
    Thread.currentThread().join();
  }

  /** Parses alternating {@code --name value} arguments. */
  static Map<String, String> args(String[] args) {
    Map<String, String> r = new LinkedHashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      if (!args[i].startsWith("--") || i + 1 >= args.length)
        throw new IllegalArgumentException("expected --option value");
      r.put(args[i].substring(2), args[i + 1]);
    }
    return r;
  }

  /** Returns a required option with a focused error message. */
  static String require(Map<String, String> a, String key) {
    String value = a.get(key);
    if (value == null) throw new IllegalArgumentException("missing --" + key);
    return value;
  }
}
