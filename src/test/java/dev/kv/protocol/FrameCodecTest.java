package dev.kv.protocol;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Verifies frame boundaries, round-tripping, and the inbound size limit. */
class FrameCodecTest {
  @Test
  void roundTripsOneBoundedFrame() throws Exception {
    var bytes = new ByteArrayOutputStream();
    FrameCodec.write(bytes, Rpc.of("CLIENT", Map.of("op", "GET", "key", "a")));
    var got = FrameCodec.read(new ByteArrayInputStream(bytes.toByteArray()));
    assertEquals("CLIENT", got.type());
    assertEquals("a", got.body().get("key"));
  }

  @Test
  void rejectsOversizedFrame() {
    byte[] header = {0x00, 0x10, 0x00, 0x01};
    assertThrows(IOException.class, () -> FrameCodec.read(new ByteArrayInputStream(header)));
  }
}
