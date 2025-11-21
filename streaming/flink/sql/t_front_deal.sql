CREATE TABLE IF NOT EXISTS fact_front_deal (
    C_ACCOUNT_CODE   STRING NOT NULL,
    C_DATE           DATE        NOT NULL,
    C_MONTH          INT         NULL,
    C_YEAR           INT         NULL,
    C_MARGIN_DEBT    DOUBLE      NULL,
    C_MBLINK_DEBT    DOUBLE      NULL,
    C_UTTB           DOUBLE      NULL,
    C_GTGD           DOUBLE      NULL,
    C_RANKING_KEY    STRING NULL,
    PRIMARY KEY (C_ACCOUNT_CODE)
)
DISTRIBUTED BY HASH(C_ACCOUNT_CODE) BUCKETS 10
PROPERTIES (
    "replication_num" = "2"
);
