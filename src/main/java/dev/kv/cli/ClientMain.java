package dev.kv.cli;

import dev.kv.protocol.FrameCodec;
import dev.kv.protocol.Rpc;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Command-line client for the public key-value operations.
 */
public final class ClientMain {
  public static void main(String[] args) {
    String seed = "127.0.0.1:8001";
    int commandAt = 0;
    
    while (commandAt < args.length && args[commandAt].startsWith("--")) {
        if ("--seed".equals(args[commandAt]) && commandAt + 1 < args.length) {
            seed = args[commandAt + 1];
        }
        commandAt += 2;
    }
    
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
    } else if (!"STATUS".equals(command)) {
      throw new IllegalArgumentException("unknown command");
    }
    
    Rpc response = call(seed, Rpc.of("CLIENT", body));
    
    if (response == null) {
      System.err.println("UNAVAILABLE");
      System.exit(2);
    } else {
      System.out.println(response.type() + " " + response.body());
    }
  }

  private static void need(String[] a, int at, int n) {
    if (a.length < at + n) throw new IllegalArgumentException("not enough command arguments");
  }

  public static Rpc call(String address, Rpc q) {
    try (Socket s = new Socket()) {
      int colon = address.lastIndexOf(':');
      InetSocketAddress a = new InetSocketAddress(
          address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)));
      s.connect(a, 500);
      s.setSoTimeout(1200);
      FrameCodec.write(s.getOutputStream(), q);
      return FrameCodec.read(s.getInputStream());
    } catch (IOException e) {
      return null;
    }
  }
}
