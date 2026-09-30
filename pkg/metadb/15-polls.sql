-- v15 (compatible with v11+): Remember which message shows each Messenger poll

CREATE TABLE meta_poll (
    bridge_id  TEXT   NOT NULL,
    poll_id    BIGINT NOT NULL,
    thread_key BIGINT NOT NULL,
    message_id TEXT   NOT NULL,

    PRIMARY KEY (bridge_id, poll_id)
);
