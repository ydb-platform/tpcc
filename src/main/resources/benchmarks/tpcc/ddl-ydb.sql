--jdbc:SCHEME
CREATE TABLE warehouse (
    W_ID       Int32          NOT NULL,
    W_YTD      Double,
    W_TAX      Double,
    W_NAME     Text,
    W_STREET_1 Text,
    W_STREET_2 Text,
    W_CITY     Text,
    W_STATE    Text,
    W_ZIP      Text,
    PRIMARY KEY (W_ID)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

--jdbc:SCHEME
CREATE TABLE item (
    I_ID    Int32           NOT NULL,
    I_NAME  Text,
    I_PRICE Double,
    I_DATA  Text,
    I_IM_ID Int32,
    PRIMARY KEY (I_ID)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

--jdbc:SCHEME
CREATE TABLE stock (
    S_W_ID       Int32           NOT NULL,
    S_I_ID       Int32           NOT NULL,
    S_QUANTITY   Int32,
    S_YTD        Double,
    S_ORDER_CNT  Int32,
    S_REMOTE_CNT Int32,
    S_DATA       Text,
    S_DIST_01    Text,
    S_DIST_02    Text,
    S_DIST_03    Text,
    S_DIST_04    Text,
    S_DIST_05    Text,
    S_DIST_06    Text,
    S_DIST_07    Text,
    S_DIST_08    Text,
    S_DIST_09    Text,
    S_DIST_10    Text,
    PRIMARY KEY (S_W_ID, S_I_ID)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);

--jdbc:SCHEME
CREATE TABLE district (
    D_W_ID      Int32            NOT NULL,
    D_ID        Int32            NOT NULL,
    D_YTD       Double,
    D_TAX       Double,
    D_NEXT_O_ID Int32,
    D_NAME      Text,
    D_STREET_1  Text,
    D_STREET_2  Text,
    D_CITY      Text,
    D_STATE     Text,
    D_ZIP       Text,
    PRIMARY KEY (D_W_ID, D_ID)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

--jdbc:SCHEME
CREATE TABLE customer (
    C_W_ID         Int32            NOT NULL,
    C_D_ID         Int32            NOT NULL,
    C_ID           Int32            NOT NULL,
    C_DISCOUNT     Double,
    C_CREDIT       Text,
    C_LAST         Text,
    C_FIRST        Text,
    C_CREDIT_LIM   Double,
    C_BALANCE      Double,
    C_YTD_PAYMENT  Double,
    C_PAYMENT_CNT  Int32,
    C_DELIVERY_CNT Int32,
    C_STREET_1     Text,
    C_STREET_2     Text,
    C_CITY         Text,
    C_STATE        Text,
    C_ZIP          Text,
    C_PHONE        Text,
    C_SINCE        Timestamp,
    C_MIDDLE       Text,
    C_DATA         Text,

    PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

--jdbc:SCHEME
CREATE TABLE history (
    H_C_ID      Int32,
    H_C_D_ID    Int32,
    H_C_W_ID    Int32,
    H_D_ID      Int32,
    H_W_ID      Int32,
    H_DATE      Timestamp,
    H_AMOUNT    Double,
    H_DATA      Text,
    H_C_NANO_TS Int64        NOT NULL,

    PRIMARY KEY (H_C_ID, H_C_NANO_TS)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

--jdbc:SCHEME
CREATE TABLE oorder (
    O_W_ID       Int32       NOT NULL,
    O_D_ID       Int32       NOT NULL,
    O_ID         Int32       NOT NULL,
    O_C_ID       Int32,
    O_CARRIER_ID Int32,
    O_OL_CNT     Int32,
    O_ALL_LOCAL  Int32,
    O_ENTRY_D    Timestamp,

    PRIMARY KEY (O_W_ID, O_D_ID, O_ID)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

--jdbc:SCHEME
CREATE TABLE new_order (
    NO_W_ID Int32 NOT NULL,
    NO_D_ID Int32 NOT NULL,
    NO_O_ID Int32 NOT NULL,

    PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

--jdbc:SCHEME
CREATE TABLE order_line (
    OL_W_ID        Int32           NOT NULL,
    OL_D_ID        Int32           NOT NULL,
    OL_O_ID        Int32           NOT NULL,
    OL_NUMBER      Int32           NOT NULL,
    OL_I_ID        Int32,
    OL_DELIVERY_D  Timestamp,
    OL_AMOUNT      Double,
    OL_SUPPLY_W_ID Int32,
    OL_QUANTITY    Double,
    OL_DIST_INFO   Text,

    PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
);
