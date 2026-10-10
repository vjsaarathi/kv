package dev.kv.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.ByteBuffer;

/**
 * Converts {@link Rpc} values to and from the TCP wire format.
 *
 * <p>TCP is a byte stream, not a message protocol. The four-byte prefix tells the receiver exactly
 * where one JSON request ends. The size limit prevents a peer from allocating arbitrarily large
 * buffers from a malformed prefix.
 */
public final class FrameCodec {
  public static final int MAX_FRAME = 1_048_576;
  public static final ObjectMapper JSON = new ObjectMapper();

  private FrameCodec() {}

  /** Serializes one complete frame and flushes it so a waiting peer can read it. */
  public static void write(OutputStream out, Rpc rpc) throws IOException {
    byte[] bytes = JSON.writeValueAsBytes(rpc);
    if (bytes.length > MAX_FRAME) throw new IOException("frame too large");
    out.write(ByteBuffer.allocate(4).putInt(bytes.length).array());
    out.write(bytes);
    out.flush();
  }

  /**
   * Reads one complete frame. Returns {@code null} only when the peer closes the stream before
   * starting a new frame; partial frames are protocol errors.
   */
  public static Rpc read(InputStream in) throws IOException {
    byte[] header = in.readNBytes(4);
    if (header.length == 0) return null;
    if (header.length != 4) throw new EOFException("truncated frame header");
    int size = ByteBuffer.wrap(header).getInt();
    if (size < 0 || size > MAX_FRAME) throw new IOException("invalid frame size: " + size);
    byte[] bytes = in.readNBytes(size);
    if (bytes.length != size) throw new EOFException("truncated frame body");
    return JSON.readValue(bytes, Rpc.class);
  }
}
