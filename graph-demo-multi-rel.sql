-- =============================================================
-- Multi-Relation Property Graph Demo
-- 目标：演示“实体间多种关系”，不仅仅是 transfer
-- =============================================================

-- 清理（可重复执行）
BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH bank_graph_multi_rel'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE account_merchant_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE account_device_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE person_knows_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE person_account_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE transfers_mr PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE merchants_mr PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE devices_mr PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE accounts_mr PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE persons_mr PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- 1) 顶点：Person / Account / Device / Merchant
CREATE TABLE persons_mr (
  person_id NUMBER PRIMARY KEY,
  full_name VARCHAR2(120),
  profile   JSON
);

CREATE TABLE accounts_mr (
  account_number NUMBER PRIMARY KEY,
  account_type   VARCHAR2(40),
  meta_json      JSON
);

CREATE TABLE devices_mr (
  device_id   NUMBER PRIMARY KEY,
  device_name VARCHAR2(120),
  device_json JSON
);

CREATE TABLE merchants_mr (
  merchant_id   NUMBER PRIMARY KEY,
  merchant_name VARCHAR2(120),
  merchant_json JSON
);

INSERT INTO persons_mr VALUES (1, 'Camille Nichols', '{"city":"Larry Islands","risk":"LOW"}');
INSERT INTO persons_mr VALUES (2, 'Jake Zimmerman', '{"city":"Oracle City","risk":"MEDIUM"}');
INSERT INTO persons_mr VALUES (3, 'Nikita Ivanov', '{"city":"Vectorville","risk":"HIGH"}');
INSERT INTO persons_mr VALUES (4, 'Liam O''Connor', '{"city":"Json Town","risk":"LOW"}');
INSERT INTO persons_mr VALUES (5, 'Sophia Chen', '{"city":"Peachtree","risk":"MEDIUM"}');
INSERT INTO persons_mr VALUES (6, 'Noah Garcia', '{"city":"Wind City","risk":"LOW"}');
INSERT INTO persons_mr VALUES (7, 'Emma Patel', '{"city":"Austin","risk":"HIGH"}');
INSERT INTO persons_mr VALUES (8, 'Oliver Smith', '{"city":"Bay Area","risk":"MEDIUM"}');

INSERT INTO accounts_mr VALUES (1001, 'CHECKING', '{"channel":"MOBILE","kyc_tier":"T2"}');
INSERT INTO accounts_mr VALUES (2090, 'SAVINGS',  '{"channel":"BRANCH","kyc_tier":"T1"}');
INSERT INTO accounts_mr VALUES (8021, 'CHECKING', '{"channel":"WEB","kyc_tier":"T3"}');
INSERT INTO accounts_mr VALUES (10039,'BROKERAGE','{"channel":"API","kyc_tier":"T3"}');
INSERT INTO accounts_mr VALUES (12001,'CHECKING', '{"channel":"MOBILE","kyc_tier":"T2"}');
INSERT INTO accounts_mr VALUES (12002,'SAVINGS',  '{"channel":"WEB","kyc_tier":"T2"}');
INSERT INTO accounts_mr VALUES (13001,'CHECKING', '{"channel":"BRANCH","kyc_tier":"T1"}');
INSERT INTO accounts_mr VALUES (14001,'BROKERAGE','{"channel":"API","kyc_tier":"T3"}');

INSERT INTO devices_mr VALUES (501, 'iPhone-15', '{"os":"iOS","trust_score":90}');
INSERT INTO devices_mr VALUES (502, 'ThinkPad-T14', '{"os":"Windows","trust_score":72}');
INSERT INTO devices_mr VALUES (503, 'Linux-Server', '{"os":"Linux","trust_score":68}');
INSERT INTO devices_mr VALUES (504, 'Pixel-8', '{"os":"Android","trust_score":75}');
INSERT INTO devices_mr VALUES (505, 'MacBook-Pro', '{"os":"macOS","trust_score":82}');
INSERT INTO devices_mr VALUES (506, 'iPad-Air', '{"os":"iPadOS","trust_score":78}');

INSERT INTO merchants_mr VALUES (901, 'CryptoX', '{"mcc":"6051","risk_level":"HIGH"}');
INSERT INTO merchants_mr VALUES (902, 'CityMart', '{"mcc":"5411","risk_level":"LOW"}');
INSERT INTO merchants_mr VALUES (903, 'TravelNow', '{"mcc":"4722","risk_level":"MEDIUM"}');
INSERT INTO merchants_mr VALUES (904, 'TechHub', '{"mcc":"5732","risk_level":"LOW"}');


-- 2) 边：多关系定义
-- 2.1 Account -> Account (TRANSFER)
CREATE TABLE transfers_mr (
  transfer_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_account NUMBER NOT NULL,
  to_account   NUMBER NOT NULL,
  transfer_time TIMESTAMP,
  amount NUMBER(12,2),
  event_json JSON,
  CONSTRAINT fk_tmr_from FOREIGN KEY (from_account) REFERENCES accounts_mr(account_number),
  CONSTRAINT fk_tmr_to   FOREIGN KEY (to_account)   REFERENCES accounts_mr(account_number)
);

INSERT INTO transfers_mr (from_account,to_account,transfer_time,amount,event_json) VALUES
  (10039, 8021, TIMESTAMP '2026-03-01 10:10:00', 1000.00, '{"channel":"API"}');
INSERT INTO transfers_mr (from_account,to_account,transfer_time,amount,event_json) VALUES
  (8021, 1001, TIMESTAMP '2026-03-01 11:00:00', 3200.50, '{"channel":"WEB"}');
INSERT INTO transfers_mr (from_account,to_account,transfer_time,amount,event_json) VALUES
  (1001, 2090, TIMESTAMP '2026-03-01 12:20:00', 9999.50, '{"channel":"MOBILE"}');
INSERT INTO transfers_mr (from_account,to_account,transfer_time,amount,event_json) VALUES
  (12001, 13001, TIMESTAMP '2026-03-01 13:05:00', 780.00, '{"channel":"MOBILE"}');
INSERT INTO transfers_mr (from_account,to_account,transfer_time,amount,event_json) VALUES
  (13001, 12002, TIMESTAMP '2026-03-01 14:40:00', 1200.00, '{"channel":"BRANCH"}');
INSERT INTO transfers_mr (from_account,to_account,transfer_time,amount,event_json) VALUES
  (14001, 10039, TIMESTAMP '2026-03-01 15:20:00', 12500.00, '{"channel":"API"}');

-- 2.2 Person -> Account (OWNS)
CREATE TABLE person_account_rel (
  rel_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  person_id NUMBER NOT NULL,
  account_number NUMBER NOT NULL,
  rel_json JSON,
  CONSTRAINT fk_par_person FOREIGN KEY (person_id) REFERENCES persons_mr(person_id),
  CONSTRAINT fk_par_account FOREIGN KEY (account_number) REFERENCES accounts_mr(account_number)
);

INSERT INTO person_account_rel (person_id, account_number, rel_json) VALUES (1, 10039, '{"since":"2020-12-15"}');
INSERT INTO person_account_rel (person_id, account_number, rel_json) VALUES (2, 1001,  '{"since":"2000-01-01"}');
INSERT INTO person_account_rel (person_id, account_number, rel_json) VALUES (3, 8021,  '{"since":"2005-03-20"}');
INSERT INTO person_account_rel (person_id, account_number, rel_json) VALUES (4, 2090,  '{"since":"2004-12-15"}');
INSERT INTO person_account_rel (person_id, account_number, rel_json) VALUES (5, 12001, '{"since":"2018-09-21"}');
INSERT INTO person_account_rel (person_id, account_number, rel_json) VALUES (5, 12002, '{"since":"2019-11-02"}');
INSERT INTO person_account_rel (person_id, account_number, rel_json) VALUES (6, 13001, '{"since":"2021-05-18"}');
INSERT INTO person_account_rel (person_id, account_number, rel_json) VALUES (7, 14001, '{"since":"2022-07-07"}');

-- 2.3 Person -> Person (KNOWS)
CREATE TABLE person_knows_rel (
  rel_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  src_person_id NUMBER NOT NULL,
  dst_person_id NUMBER NOT NULL,
  rel_strength NUMBER(4,2),
  rel_json JSON,
  CONSTRAINT fk_pkr_src FOREIGN KEY (src_person_id) REFERENCES persons_mr(person_id),
  CONSTRAINT fk_pkr_dst FOREIGN KEY (dst_person_id) REFERENCES persons_mr(person_id)
);

INSERT INTO person_knows_rel (src_person_id, dst_person_id, rel_strength, rel_json) VALUES
  (1, 3, 0.70, '{"context":"work"}');
INSERT INTO person_knows_rel (src_person_id, dst_person_id, rel_strength, rel_json) VALUES
  (3, 2, 0.85, '{"context":"project"}');
INSERT INTO person_knows_rel (src_person_id, dst_person_id, rel_strength, rel_json) VALUES
  (2, 4, 0.60, '{"context":"community"}');
INSERT INTO person_knows_rel (src_person_id, dst_person_id, rel_strength, rel_json) VALUES
  (5, 6, 0.75, '{"context":"colleague"}');
INSERT INTO person_knows_rel (src_person_id, dst_person_id, rel_strength, rel_json) VALUES
  (6, 7, 0.55, '{"context":"forum"}');
INSERT INTO person_knows_rel (src_person_id, dst_person_id, rel_strength, rel_json) VALUES
  (7, 8, 0.80, '{"context":"startup"}');

-- 2.4 Account -> Device (USES_DEVICE)
CREATE TABLE account_device_rel (
  rel_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_number NUMBER NOT NULL,
  device_id NUMBER NOT NULL,
  last_seen TIMESTAMP,
  rel_json JSON,
  CONSTRAINT fk_adr_account FOREIGN KEY (account_number) REFERENCES accounts_mr(account_number),
  CONSTRAINT fk_adr_device FOREIGN KEY (device_id) REFERENCES devices_mr(device_id)
);

INSERT INTO account_device_rel (account_number,device_id,last_seen,rel_json) VALUES
  (10039, 503, TIMESTAMP '2026-03-01 09:00:00', '{"ip":"10.0.1.9"}');
INSERT INTO account_device_rel (account_number,device_id,last_seen,rel_json) VALUES
  (8021,  502, TIMESTAMP '2026-03-01 10:45:00', '{"ip":"10.0.1.8"}');
INSERT INTO account_device_rel (account_number,device_id,last_seen,rel_json) VALUES
  (1001,  501, TIMESTAMP '2026-03-01 12:10:00', '{"ip":"10.0.1.7"}');
INSERT INTO account_device_rel (account_number,device_id,last_seen,rel_json) VALUES
  (12001, 504, TIMESTAMP '2026-03-01 13:00:00', '{"ip":"10.0.2.1"}');
INSERT INTO account_device_rel (account_number,device_id,last_seen,rel_json) VALUES
  (12002, 505, TIMESTAMP '2026-03-01 14:00:00', '{"ip":"10.0.2.2"}');
INSERT INTO account_device_rel (account_number,device_id,last_seen,rel_json) VALUES
  (14001, 506, TIMESTAMP '2026-03-01 15:00:00', '{"ip":"10.0.2.3"}');

-- 2.5 Account -> Merchant (PAYS_MERCHANT)
CREATE TABLE account_merchant_rel (
  rel_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_number NUMBER NOT NULL,
  merchant_id NUMBER NOT NULL,
  txn_count NUMBER,
  rel_json JSON,
  CONSTRAINT fk_amr_account FOREIGN KEY (account_number) REFERENCES accounts_mr(account_number),
  CONSTRAINT fk_amr_merchant FOREIGN KEY (merchant_id) REFERENCES merchants_mr(merchant_id)
);

INSERT INTO account_merchant_rel (account_number,merchant_id,txn_count,rel_json) VALUES
  (10039, 901, 4, '{"total_amount":18500}');
INSERT INTO account_merchant_rel (account_number,merchant_id,txn_count,rel_json) VALUES
  (1001,  902, 9, '{"total_amount":2300}');
INSERT INTO account_merchant_rel (account_number,merchant_id,txn_count,rel_json) VALUES
  (8021,  901, 2, '{"total_amount":7000}');
INSERT INTO account_merchant_rel (account_number,merchant_id,txn_count,rel_json) VALUES
  (12001, 903, 5, '{"total_amount":4200}');
INSERT INTO account_merchant_rel (account_number,merchant_id,txn_count,rel_json) VALUES
  (12002, 904, 7, '{"total_amount":3600}');
INSERT INTO account_merchant_rel (account_number,merchant_id,txn_count,rel_json) VALUES
  (14001, 901, 3, '{"total_amount":9800}');

COMMIT;


-- 3) 创建多关系 Property Graph
CREATE PROPERTY GRAPH bank_graph_multi_rel
  VERTEX TABLES (
    persons_mr AS person
      KEY (person_id)
      PROPERTIES (person_id, full_name, profile),
    accounts_mr AS account
      KEY (account_number)
      PROPERTIES (account_number, account_type, meta_json),
    devices_mr AS device
      KEY (device_id)
      PROPERTIES (device_id, device_name, device_json),
    merchants_mr AS merchant
      KEY (merchant_id)
      PROPERTIES (merchant_id, merchant_name, merchant_json)
  )
  EDGE TABLES (
    transfers_mr AS transfer
      KEY (transfer_id)
      SOURCE KEY (from_account) REFERENCES account (account_number)
      DESTINATION KEY (to_account) REFERENCES account (account_number)
      PROPERTIES (transfer_id, transfer_time, amount, event_json),

    person_account_rel AS owns
      KEY (rel_id)
      SOURCE KEY (person_id) REFERENCES person (person_id)
      DESTINATION KEY (account_number) REFERENCES account (account_number)
      PROPERTIES (rel_id, rel_json),

    person_knows_rel AS knows
      KEY (rel_id)
      SOURCE KEY (src_person_id) REFERENCES person (person_id)
      DESTINATION KEY (dst_person_id) REFERENCES person (person_id)
      PROPERTIES (rel_id, rel_strength, rel_json),

    account_device_rel AS uses_device
      KEY (rel_id)
      SOURCE KEY (account_number) REFERENCES account (account_number)
      DESTINATION KEY (device_id) REFERENCES device (device_id)
      PROPERTIES (rel_id, last_seen, rel_json),

    account_merchant_rel AS pays_merchant
      KEY (rel_id)
      SOURCE KEY (account_number) REFERENCES account (account_number)
      DESTINATION KEY (merchant_id) REFERENCES merchant (merchant_id)
      PROPERTIES (rel_id, txn_count, rel_json)
  );


-- =============================================================
-- 4) 可视化查询 A：Account -> Account 转账
-- =============================================================
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  from_account,
  to_account,
  amount,
  channel
FROM GRAPH_TABLE (
  bank_graph_multi_rel
  MATCH (a1 IS account) -[e IS transfer]-> (a2 IS account)
  COLUMNS (
    VERTEX_ID(a1) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(a1) AS source_vertex_id,
    VERTEX_ID(a2) AS destination_vertex_id,
    a1.account_number AS from_account,
    a2.account_number AS to_account,
    e.amount AS amount,
    e.event_json.channel.string() AS channel
  )
);


-- =============================================================
-- 5) 可视化查询 B：Person -> Account 拥有关系
-- =============================================================
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  person_name,
  account_number,
  account_type
FROM GRAPH_TABLE (
  bank_graph_multi_rel
  MATCH (p IS person) -[e IS owns]-> (a IS account)
  COLUMNS (
    VERTEX_ID(p) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(p) AS source_vertex_id,
    VERTEX_ID(a) AS destination_vertex_id,
    p.full_name AS person_name,
    a.account_number AS account_number,
    a.account_type AS account_type
  )
);


-- =============================================================
-- 6) 可视化查询 C：Account -> Device / Merchant 多关系
-- =============================================================
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  account_number,
  target_name,
  relation_type,
  value_metric
FROM GRAPH_TABLE (
  bank_graph_multi_rel
  MATCH (a IS account) -[e]-> (t)
  COLUMNS (
    VERTEX_ID(a) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(a) AS source_vertex_id,
    VERTEX_ID(t) AS destination_vertex_id,
    a.account_number AS account_number,
    CASE
      WHEN t.device_name IS NOT NULL THEN t.device_name
      ELSE t.merchant_name
    END AS target_name,
    CASE
      WHEN e.last_seen IS NOT NULL THEN 'USES_DEVICE'
      WHEN e.txn_count IS NOT NULL THEN 'PAYS_MERCHANT'
      ELSE 'OTHER'
    END AS relation_type,
    NVL(e.txn_count, JSON_VALUE(t.device_json, '$.trust_score' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR)) AS value_metric
  )
)
WHERE relation_type IN ('USES_DEVICE', 'PAYS_MERCHANT');


-- =============================================================
-- 7) 分析查询：通过“人际关系 + 账户关系”找间接资金路径
-- =============================================================
SELECT *
FROM GRAPH_TABLE (
  bank_graph_multi_rel
  MATCH (p1 IS person) -[IS knows]-> (p2 IS person),
        (p1) -[IS owns]-> (a1 IS account),
        (p2) -[IS owns]-> (a2 IS account),
        (a1) -[e IS transfer]-> (a2)
  COLUMNS (
    p1.full_name AS src_person,
    p2.full_name AS dst_person,
    a1.account_number AS src_account,
    a2.account_number AS dst_account,
    e.amount AS transfer_amount
  )
)
ORDER BY transfer_amount DESC;
