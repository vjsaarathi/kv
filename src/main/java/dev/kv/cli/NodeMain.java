package dev.kv.cli;

import dev.kv.protocol.FrameCodec;
import dev.kv.protocol.Rpc;
import dev.kv.storage.StateMachine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A simple single-node in-memory key-value server.
 * This is Part 1 of the tutorial: no persistence, no replication.
 */
public final class NodeMain {
  private final String bind;
  private final StateMachine machine = new StateMachine();
  private final ExecutorService workers = Executors.newCachedThreadPool();
  private volatile boolean running;
  private ServerSocket socket;

  public NodeMain(String bind) {
    this.bind = bind;
  }

  public void start() throws IOException {
    int colon = bind.lastIndexOf(':');
    InetSocketAddress address = new InetSocketAddress(
        bind.substring(0, colon), Integer.parseInt(bind.substring(colon + 1)));
    
    socket = new ServerSocket();
    socket.bind(address);
    running = true;
    
    System.out.println("Single-node server listening on " + bind);
    
    workers.submit(() -> {
      while (running) {
        try {
          Socket s = socket.accept();
          workers.submit(() -> serve(s));
        } catch (IOException e) {
          if (running) e.printStackTrace();
        }
      }
    });
  }

  private void serve(Socket s) {
    try (s) {
      s.setSoTimeout(1500);
      Rpc request = FrameCodec.read(s.getInputStream());
      if (request != null) {
        Rpc response = dispatch(request);
        FrameCodec.write(s.getOutputStream(), response);
      }
    } catch (IOException ignored) {
    }
  }

  private synchronized Rpc dispatch(Rpc q) {
    if (!"CLIENT".equals(q.type())) {
      return err("BAD_REQUEST", "unknown rpc");
    }
    
    try {
      String op = str(q.body(), "op");
      if ("STATUS".equals(op)) return status();
      
      return switch (op) {
        case "PUT", "DELETE" -> mutate(op, str(q.body(), "key"), str(q.body(), "value"));
        case "GET" -> get(str(q.body(), "key"));
        default -> err("BAD_REQUEST", "unknown command");
      };
    } catch (Exception e) {
      return err("ERROR", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
    }
  }

  private Rpc mutate(String op, String key, String value) {
    machine.apply(op, key, value);
    return Rpc.of("OK", Map.of("message", "success"));
  }

  private Rpc get(String key) {
    Optional<String> value = machine.get(key);
    Map<String, Object> b = new HashMap<>();
    b.put("found", value.isPresent());
    b.put("value", value.orElse(null));
    return Rpc.of("VALUE", b);
  }

  private Rpc status() {
    Map<String, Object> b = new HashMap<>();
    b.put("role", "SINGLE_NODE");
    b.put("data", machine.copy());
    return Rpc.of("STATUS", b);
  }

  private Rpc err(String code, String message) {
    return Rpc.of(code, Map.of("message", message));
  }

  private static String str(Map<String, Object> b, String k) {
    Object v = b.get(k);
    return v == null ? null : String.valueOf(v);
  }

  public void close() {
    running = false;
    try {
      if (socket != null) socket.close();
    } catch (IOException ignored) {}
    workers.shutdownNow();
  }

  public static void main(String[] args) throws Exception {
    String bind = "127.0.0.1:8001";
    for (int i = 0; i < args.length; i += 2) {
      if ("--bind".equals(args[i]) && i + 1 < args.length) {
        bind = args[i + 1];
      }
    }
    NodeMain node = new NodeMain(bind);
    node.start();
    Runtime.getRuntime().addShutdownHook(new Thread(node::close));
    Thread.currentThread().join();
  }
}
