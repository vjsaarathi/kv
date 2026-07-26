package dev.kv.raft;

/**
 * One ordered command in the replicated Raft log.
 *
 * <p>The list position is its Raft index; index zero is implicit and therefore absent. {@code term}
 * identifies the leader that created the entry. {@code op} is either {@code PUT} or {@code DELETE};
 * delete entries have no useful value.
 */
public record LogEntry(long term, String op, String key, String value) {}
