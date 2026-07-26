package dev.kv.storage;

import dev.kv.protocol.FrameCodec;
import dev.kv.raft.LogEntry;
import java.io.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Owns one node's durable Raft files.
 *
 * <p>Raft requires the current term and vote to survive a restart; otherwise a node could vote
 * twice in one term. The committed index is also stored so the state machine can be reconstructed
 * by replaying exactly the committed log prefix. Metadata replacement uses a temporary file and
 * atomic rename. Log records are framed individually, making normal log extension append-only.
 */
public final class PersistentStore {
  /** The small fixed-size portion of Raft state that must survive a restart. */
  public record Meta(long term, String votedFor, long commitIndex) {}

  /** The complete recovered durable state returned during node construction. */
  public record Loaded(Meta meta, List<LogEntry> log) {}

  private final Path dir, metaFile, logFile;

  public PersistentStore(Path dir) throws IOException {
    this.dir = dir;
    Files.createDirectories(dir);
    metaFile = dir.resolve("meta.json");
    logFile = dir.resolve("raft-log.json");
  }

  /** Loads and validates metadata plus every framed log record. */
  public Loaded load() throws IOException {
    Meta m =
        Files.exists(metaFile)
            ? FrameCodec.JSON.readValue(metaFile.toFile(), Meta.class)
            : new Meta(0, null, 0);
    List<LogEntry> l = new ArrayList<>();
    if (Files.exists(logFile))
      try (InputStream in = Files.newInputStream(logFile)) {
        while (true) {
          byte[] header = in.readNBytes(4);
          if (header.length == 0) break;
          if (header.length != 4) throw new EOFException("truncated log frame");
          int size = java.nio.ByteBuffer.wrap(header).getInt();
          if (size < 0 || size > FrameCodec.MAX_FRAME) throw new IOException("bad log frame");
          byte[] body = in.readNBytes(size);
          if (body.length != size) throw new EOFException("truncated log entry");
          l.add(FrameCodec.JSON.readValue(body, LogEntry.class));
        }
      }
    if (m.commitIndex() < 0 || m.commitIndex() > l.size())
      throw new IOException("invalid committed index");
    return new Loaded(m, new ArrayList<>(l));
  }

  /** Persists a new term, vote, or commit index before its acknowledgement is sent. */
  public void saveMeta(Meta meta) throws IOException {
    atomic(metaFile, meta);
  }

  /** Normal Raft appends use this path; truncation repairs rewrite the durable prefix. */
  public void appendLog(LogEntry entry) throws IOException {
    try (OutputStream out =
        Files.newOutputStream(logFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      byte[] bytes = FrameCodec.JSON.writeValueAsBytes(entry);
      out.write(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());
      out.write(bytes);
      out.flush();
    }
  }

  /** Rewrites the log after an AppendEntries conflict removes a divergent suffix. */
  public void saveLog(List<LogEntry> log) throws IOException {
    Path temp = Files.createTempFile(dir, "raft-log", ".tmp");
    try (OutputStream out = Files.newOutputStream(temp)) {
      for (LogEntry entry : log) {
        byte[] bytes = FrameCodec.JSON.writeValueAsBytes(entry);
        out.write(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());
        out.write(bytes);
      }
    }
    try {
      Files.move(
          temp, logFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(temp, logFile, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private void atomic(Path target, Object value) throws IOException {
    Path temp = Files.createTempFile(dir, target.getFileName().toString(), ".tmp");
    try {
      FrameCodec.JSON.writeValue(temp.toFile(), value);
      Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
