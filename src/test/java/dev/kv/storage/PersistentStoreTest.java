package dev.kv.storage;

import static org.junit.jupiter.api.Assertions.*;

import dev.kv.raft.LogEntry;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies that the durable Raft state can be reconstructed after process exit. */
class PersistentStoreTest {
  @Test
  void restoresTermVoteCommitAndLog() throws Exception {
    var dir = Files.createTempDirectory("raft-store");
    var store = new PersistentStore(dir);
    store.saveLog(List.of(new LogEntry(2, "PUT", "k", "v")));
    store.saveMeta(new PersistentStore.Meta(2, "n1", 1));
    var restored = new PersistentStore(dir).load();
    assertEquals(2, restored.meta().term());
    assertEquals("n1", restored.meta().votedFor());
    assertEquals(1, restored.meta().commitIndex());
    assertEquals("v", restored.log().getFirst().value());
  }
}
