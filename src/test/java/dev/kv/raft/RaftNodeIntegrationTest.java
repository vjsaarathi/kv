package dev.kv.raft;

import static org.junit.jupiter.api.Assertions.*;

import dev.kv.protocol.Rpc;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.*;
import org.junit.jupiter.api.Test;

/** Starts real loopback nodes to verify cross-node protocol behavior. */
class RaftNodeIntegrationTest {
  @Test
  void electsReplicatesAndServesBarrierRead() throws Exception {
    int p1 = port(), p2 = port(), p3 = port();
    Map<String, String> peers =
        Map.of("n1", "127.0.0.1:" + p1, "n2", "127.0.0.1:" + p2, "n3", "127.0.0.1:" + p3);
    try (var n1 = new RaftNode("n1", peers.get("n1"), peers, Files.createTempDirectory("n1"));
        var n2 = new RaftNode("n2", peers.get("n2"), peers, Files.createTempDirectory("n2"));
        var n3 = new RaftNode("n3", peers.get("n3"), peers, Files.createTempDirectory("n3"))) {
      n1.start();
      n2.start();
      n3.start();
      String leader = awaitLeader(peers);
      String follower =
          peers.values().stream().filter(a -> !a.equals(leader)).findFirst().orElseThrow();
      assertEquals(
          "NOT_LEADER",
          RaftNode.call(
                  follower,
                  Rpc.of("CLIENT", Map.of("op", "PUT", "key", "redirect", "value", "yes")))
              .type());
      Rpc put =
          RaftNode.call(
              leader, Rpc.of("CLIENT", Map.of("op", "PUT", "key", "language", "value", "java")));
      assertEquals("OK", put.type());
      Rpc get = RaftNode.call(leader, Rpc.of("CLIENT", Map.of("op", "GET", "key", "language")));
      assertEquals("VALUE", get.type());
      assertEquals(true, get.body().get("found"));
      assertEquals("java", get.body().get("value"));
    }
  }

  @Test
  void grantsOnlyOneVotePerTermAndRejectsBadAppendPrefix() throws Exception {
    int port = port();
    Map<String, String> peers = Map.of("n1", "127.0.0.1:" + port);
    try (var node = new RaftNode("n1", peers.get("n1"), peers, Files.createTempDirectory("vote"))) {
      node.start();
      String address = peers.get("n1");
      Rpc first =
          RaftNode.call(
              address,
              Rpc.of("VOTE", Map.of("term", 1, "candidate", "a", "lastIndex", 0, "lastTerm", 0)));
      Rpc second =
          RaftNode.call(
              address,
              Rpc.of("VOTE", Map.of("term", 1, "candidate", "b", "lastIndex", 0, "lastTerm", 0)));
      assertEquals(true, first.body().get("granted"));
      assertEquals(false, second.body().get("granted"));
      Rpc append =
          RaftNode.call(
              address,
              Rpc.of(
                  "APPEND",
                  Map.of(
                      "term",
                      1,
                      "leader",
                      "a",
                      "prevIndex",
                      1,
                      "prevTerm",
                      99,
                      "entries",
                      List.of(),
                      "leaderCommit",
                      0,
                      "readContext",
                      "x")));
      assertEquals(false, append.body().get("success"));
    }
  }

  @Test
  void restoresCommittedStateAfterRestart() throws Exception {
    int port = port();
    Map<String, String> peers = Map.of("n1", "127.0.0.1:" + port);
    var dir = Files.createTempDirectory("restart");
    try (var node = new RaftNode("n1", peers.get("n1"), peers, dir)) {
      node.start();
      String leader = awaitLeader(peers);
      assertEquals(
          "OK",
          RaftNode.call(
                  leader, Rpc.of("CLIENT", Map.of("op", "PUT", "key", "survives", "value", "yes")))
              .type());
    }
    try (var restored = new RaftNode("n1", peers.get("n1"), peers, dir)) {
      assertEquals("yes", ((Map<?, ?>) restored.debugState().get("data")).get("survives"));
    }
  }

  private static String awaitLeader(Map<String, String> peers) throws Exception {
    for (int i = 0; i < 40; i++) {
      for (String a : peers.values()) {
        Rpc r = RaftNode.call(a, Rpc.of("CLIENT", Map.of("op", "STATUS")));
        if (r != null && "LEADER".equals(r.body().get("role"))) return a;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("no leader elected");
  }

  private static int port() throws Exception {
    try (var s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }
}
