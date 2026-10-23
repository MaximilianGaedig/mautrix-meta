-- v0 -> v1: Latest fork schema (tables only this fork has)

-- Which message shows each Messenger poll. Databases from before this table existed got it as the
-- fork's v15 in meta_version; legacyForkNumbering moves them here without touching the table.
CREATE TABLE meta_poll (
    bridge_id  TEXT   NOT NULL,
    poll_id    BIGINT NOT NULL,
    thread_key BIGINT NOT NULL,
    message_id TEXT   NOT NULL,

    PRIMARY KEY (bridge_id, poll_id)
);
