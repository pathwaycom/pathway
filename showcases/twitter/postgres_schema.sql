DROP TABLE IF EXISTS author_meta;
CREATE TABLE author_meta (
    tweet_to_author_id TEXT NOT NULL,
    coord_to TEXT NOT NULL,
    author_username TEXT NOT NULL,
    author_location TEXT NOT NULL,
    coord_shifted TEXT NOT NULL,
    total_responses BIGINT NOT NULL,
    total_magic_influence FLOAT NOT NULL,
    time INTEGER NOT NULL,
    diff INTEGER NOT NULL,

    PRIMARY KEY (tweet_to_author_id)
);

DROP TABLE IF EXISTS grouped;
CREATE TABLE grouped (
    tweet_to_author_id TEXT NOT NULL,
    time_bucket BIGINT NOT NULL,
    tweet_from_sentiment FLOAT NOT NULL,
    author_from_magic_influence FLOAT NOT NULL,
    responses_count BIGINT NOT NULL,
    close_count BIGINT NOT NULL,
    medium_count BIGINT NOT NULL,
    far_count BIGINT NOT NULL,
    time INTEGER NOT NULL,
    diff INTEGER NOT NULL,

    PRIMARY KEY (tweet_to_author_id, time_bucket)
);

DROP TABLE IF EXISTS tweet_pairs;
CREATE TABLE tweet_pairs (
    tweet_from_id TEXT NOT NULL,
    tweet_from_created_at TEXT NOT NULL,
    tweet_from_text TEXT NOT NULL,
    tweet_from_author_id TEXT NOT NULL,
    tweet_from_author_username TEXT NOT NULL,
    tweet_from_author_location TEXT NOT NULL,
    tweet_from_author_public_metrics TEXT NOT NULL,
    tweet_to_id TEXT NOT NULL,
    tweet_to_created_at TEXT NOT NULL,
    tweet_to_text TEXT NOT NULL,
    tweet_to_author_id TEXT NOT NULL,
    tweet_to_author_username TEXT NOT NULL,
    tweet_to_author_location TEXT NOT NULL,
    coord_from TEXT NOT NULL,
    coord_to TEXT NOT NULL,
    distance FLOAT NOT NULL,
    bucketed_distance TEXT NOT NULL,
    tweet_from_sentiment FLOAT NOT NULL,
    tweet_to_sentiment FLOAT NOT NULL,
    author_from_magic_influence FLOAT NOT NULL,
    is_good BOOLEAN NOT NULL,
    time INTEGER NOT NULL,
    diff INTEGER NOT NULL,

    PRIMARY KEY (tweet_from_id, tweet_to_id)
);