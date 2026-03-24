-- =============================================================
-- Oracle Property Graph + JSON 示例脚本
-- 目标：在关系模型上体现“图 + JSON（OSON）”能力
-- =============================================================

-- 清理（可重复执行）
BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH bank_graph_json'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE transfers_json PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE accounts_json PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE persons_json PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- 1) 顶点表：人员（JSON 属性）
CREATE TABLE persons_json (
  person_id NUMBER PRIMARY KEY,
  first_name VARCHAR2(100),
  last_name  VARCHAR2(100),
  profile    JSON
);

INSERT INTO persons_json VALUES (
  1,
  'Camille',
  'Nichols',
  '{
     "address":{"line1":"101 Ellison Avenue","zip_code":10001,"city":"Larry Islands","state":"AL"},
     "risk":{"level":"LOW","score":22},
     "tags":["vip","mobile"]
   }'
);

INSERT INTO persons_json VALUES (
  2,
  'Jake',
  'Zimmerman',
  '{
     "address":{"line1":"500 Property Graph Parkway","zip_code":94065,"city":"Oracle City","state":"CA"},
     "risk":{"level":"MEDIUM","score":55},
     "tags":["web"]
   }'
);

INSERT INTO persons_json VALUES (
  3,
  'Nikita',
  'Ivanov',
  '{
     "address":{"line1":"1 Boolean Boulevard","zip_code":48201,"city":"Vectorville","state":"AZ"},
     "risk":{"level":"HIGH","score":88},
     "tags":["intl","api"]
   }'
);

INSERT INTO persons_json VALUES (
  4,
  'Liam',
  'O''Connor',
  '{
     "address":{"line1":"16 SQL Street","zip_code":9075,"city":"Json Town","state":"CA"},
     "risk":{"level":"LOW","score":15},
     "tags":["branch"]
   }'
);

INSERT INTO persons_json VALUES (
  5,
  'Sophia',
  'Chen',
  '{
     "address":{"line1":"88 Lake View Rd","zip_code":30301,"city":"Peachtree","state":"GA"},
     "risk":{"level":"MEDIUM","score":60},
     "tags":["wealth","mobile"]
   }'
);

INSERT INTO persons_json VALUES (
  6,
  'Noah',
  'Garcia',
  '{
     "address":{"line1":"320 Harbor Ave","zip_code":60601,"city":"Wind City","state":"IL"},
     "risk":{"level":"LOW","score":30},
     "tags":["salary"]
   }'
);

INSERT INTO persons_json VALUES (
  7,
  'Emma',
  'Patel',
  '{
     "address":{"line1":"9 Data Street","zip_code":73301,"city":"Austin","state":"TX"},
     "risk":{"level":"HIGH","score":91},
     "tags":["intl","crypto"]
   }'
);


-- 2) 顶点表：账户（JSON 属性）
CREATE TABLE accounts_json (
  account_number NUMBER PRIMARY KEY,
  owner_id       NUMBER NOT NULL,
  creation_date  TIMESTAMP,
  account_meta   JSON,
  CONSTRAINT fk_accounts_json_owner FOREIGN KEY (owner_id) REFERENCES persons_json(person_id)
);

INSERT INTO accounts_json VALUES (
  1001,
  2,
  TIMESTAMP '2000-01-01 14:31:00',
  '{"type":"CHECKING","channel":"MOBILE","kyc":{"tier":"T2"}}'
);

INSERT INTO accounts_json VALUES (
  2090,
  4,
  TIMESTAMP '2004-12-15 08:15:00',
  '{"type":"SAVINGS","channel":"BRANCH","kyc":{"tier":"T1"}}'
);

INSERT INTO accounts_json VALUES (
  8021,
  3,
  TIMESTAMP '2005-03-20 10:45:00',
  '{"type":"CHECKING","channel":"WEB","kyc":{"tier":"T3"}}'
);

INSERT INTO accounts_json VALUES (
  10039,
  1,
  TIMESTAMP '2020-12-15 14:17:00',
  '{"type":"BROKERAGE","channel":"API","kyc":{"tier":"T3"}}'
);

INSERT INTO accounts_json VALUES (
  12001,
  5,
  TIMESTAMP '2018-09-21 09:20:00',
  '{"type":"CHECKING","channel":"MOBILE","kyc":{"tier":"T2"}}'
);

INSERT INTO accounts_json VALUES (
  12002,
  5,
  TIMESTAMP '2019-11-02 13:10:00',
  '{"type":"SAVINGS","channel":"WEB","kyc":{"tier":"T2"}}'
);

INSERT INTO accounts_json VALUES (
  13001,
  6,
  TIMESTAMP '2021-05-18 11:05:00',
  '{"type":"CHECKING","channel":"BRANCH","kyc":{"tier":"T1"}}'
);

INSERT INTO accounts_json VALUES (
  14001,
  7,
  TIMESTAMP '2022-07-07 17:40:00',
  '{"type":"BROKERAGE","channel":"API","kyc":{"tier":"T3"}}'
);


-- 3) 边表：转账（JSON 属性）
CREATE TABLE transfers_json (
  transfer_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_account  NUMBER NOT NULL,
  to_account    NUMBER NOT NULL,
  transfer_time TIMESTAMP,
  amount        NUMBER(12,2),
  event_json    JSON,
  CONSTRAINT fk_transfers_json_src FOREIGN KEY (from_account) REFERENCES accounts_json(account_number),
  CONSTRAINT fk_transfers_json_dst FOREIGN KEY (to_account)   REFERENCES accounts_json(account_number)
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  10039, 8021, TIMESTAMP '2024-03-12 14:30:00', 1000.00,
  '{"channel":"API","device":{"os":"iOS"},"flags":{"suspicious":false},"geo":{"zip_code":94065}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  8021, 1001, TIMESTAMP '2024-03-12 16:30:00', 1500.30,
  '{"channel":"WEB","device":{"os":"Windows"},"flags":{"suspicious":false},"geo":{"zip_code":48201}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  8021, 1001, TIMESTAMP '2024-03-12 16:31:00', 3000.00,
  '{"channel":"WEB","device":{"os":"Linux"},"flags":{"suspicious":true},"geo":{"zip_code":48201}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  1001, 2090, TIMESTAMP '2024-03-12 19:03:00', 9999.50,
  '{"channel":"MOBILE","device":{"os":"Android"},"flags":{"suspicious":true},"geo":{"zip_code":94065}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  2090, 10039, TIMESTAMP '2024-03-12 19:45:00', 9900.00,
  '{"channel":"BRANCH","device":{"os":"N/A"},"flags":{"suspicious":false},"geo":{"zip_code":9075}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  12001, 13001, TIMESTAMP '2024-03-13 09:12:00', 320.45,
  '{"channel":"MOBILE","device":{"os":"Android"},"flags":{"suspicious":false},"geo":{"zip_code":30301}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  13001, 12002, TIMESTAMP '2024-03-13 11:28:00', 780.00,
  '{"channel":"BRANCH","device":{"os":"N/A"},"flags":{"suspicious":false},"geo":{"zip_code":60601}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  14001, 10039, TIMESTAMP '2024-03-13 12:01:00', 12500.00,
  '{"channel":"API","device":{"os":"Linux"},"flags":{"suspicious":true},"geo":{"zip_code":73301}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  1001, 12001, TIMESTAMP '2024-03-13 15:45:00', 2150.75,
  '{"channel":"WEB","device":{"os":"Windows"},"flags":{"suspicious":false},"geo":{"zip_code":94065}}'
);

INSERT INTO transfers_json (from_account, to_account, transfer_time, amount, event_json) VALUES (
  12002, 14001, TIMESTAMP '2024-03-13 19:22:00', 6400.00,
  '{"channel":"WEB","device":{"os":"macOS"},"flags":{"suspicious":true},"geo":{"zip_code":30301}}'
);

COMMIT;


-- 4) 创建 Property Graph
CREATE PROPERTY GRAPH bank_graph_json
  VERTEX TABLES (
    persons_json AS person
      KEY (person_id)
      PROPERTIES (first_name, last_name, profile),

    accounts_json AS account
      KEY (account_number)
      PROPERTIES (account_number, owner_id, creation_date, account_meta)
  )
  EDGE TABLES (
    transfers_json AS transfer
      KEY (transfer_id)
      SOURCE KEY (from_account) REFERENCES account (account_number)
      DESTINATION KEY (to_account) REFERENCES account (account_number)
      PROPERTIES (transfer_id, transfer_time, amount, event_json),

    accounts_json AS owner
      SOURCE KEY (account_number) REFERENCES account (account_number)
      DESTINATION KEY (owner_id) REFERENCES person (person_id)
      NO PROPERTIES
  );


-- =============================================================
-- 5) 查询示例 A：Graph Visualization（必须带 vertex_id / edge_id）
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
  bank_graph_json
  MATCH (a IS account) -[e IS transfer]-> (b IS account)
  COLUMNS (
    VERTEX_ID(a) AS vertex_id,
    EDGE_ID(e)   AS edge_id,
    VERTEX_ID(a) AS source_vertex_id,
    VERTEX_ID(b) AS destination_vertex_id,
    a.account_number AS from_account,
    b.account_number AS to_account,
    e.amount AS amount,
    e.event_json.channel.string() AS channel
  )
);


-- =============================================================
-- 6) 查询示例 B：JSON dot-notation + 图路径过滤（可直接用于 Graph Visualization）
--    说明：为避免 ORA-20000，这里显式输出 vertex_id / edge_id
-- =============================================================
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  src_name,
  dst_name,
  src_zip,
  edge_amount,
  edge_zip,
  suspicious_flag
FROM GRAPH_TABLE (
  bank_graph_json
  MATCH (p1 IS person) <-[IS owner]- (a1 IS account)
        -[e IS transfer]-> (a2 IS account) -[IS owner]-> (p2 IS person)
  WHERE p1.profile.address.zip_code.number() = 94065
    AND e.event_json.flags.suspicious.boolean() = true
  COLUMNS (
    VERTEX_ID(a1) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(a1) AS source_vertex_id,
    VERTEX_ID(a2) AS destination_vertex_id,
    p1.first_name AS src_name,
    p2.first_name AS dst_name,
    p1.profile.address.zip_code.number() AS src_zip,
    e.amount AS edge_amount,
    e.event_json.geo.zip_code.number() AS edge_zip,
    e.event_json.flags.suspicious.boolean() AS suspicious_flag
  )
)
ORDER BY edge_amount DESC;


-- =============================================================
-- 7) 查询示例 C：可直接用于 Graph Visualization（Camille 相关转账）
-- =============================================================
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  start_person,
  risk_level,
  from_account,
  to_account,
  amount,
  channel
FROM GRAPH_TABLE (
  bank_graph_json
  MATCH (p IS person) <-[IS owner]- (a1 IS account)
        -[e IS transfer]-> (a2 IS account)
  WHERE p.first_name = 'Camille'
  COLUMNS (
    VERTEX_ID(a1) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(a1) AS source_vertex_id,
    VERTEX_ID(a2) AS destination_vertex_id,
    p.first_name AS start_person,
    JSON_VALUE(p.profile, '$.risk.level' RETURNING VARCHAR2(30)) AS risk_level,
    a1.account_number AS from_account,
    a2.account_number AS to_account,
    e.amount AS amount,
    e.event_json.channel.string() AS channel
  )
)
ORDER BY amount DESC;


-- =============================================================
-- 8) 建模灵活性示例：仅给“部分”顶点/边增加 JSON 属性（无需改表结构）
--    目标：体现 schemaless 风格属性扩展 + 图查询直接消费新字段
-- =============================================================

-- 仅给 person_id=3 增加合规信息（其它人物不需要此字段）
UPDATE persons_json
SET profile = JSON_MERGEPATCH(
  profile,
  '{"compliance":{"pep":true,"watchlists":["OFAC"]}}'
)
WHERE person_id = 3;

-- 仅给 transfer_id=4 增加 AML 和商户信息（其它边不需要这些字段）
UPDATE transfers_json
SET event_json = JSON_MERGEPATCH(
  event_json,
  '{"aml":{"scenario":"SMURFING","score":92},"counterparty":{"merchant":{"mcc":"6051","name":"CryptoX"}}}'
)
WHERE transfer_id = 4;

COMMIT;

-- 图查询直接读取“可选 JSON 字段”：
-- - 部分顶点有 compliance.pep
-- - 部分边有 aml.score / merchant.mcc
-- 无需 ALTER TABLE 新增列，也无需重建图模型。
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  person_name,
  src_account,
  dst_account,
  amount,
  pep_flag,
  aml_scenario,
  aml_score,
  merchant_mcc
FROM GRAPH_TABLE (
  bank_graph_json
  MATCH (p IS person) <-[IS owner]- (a1 IS account)
        -[e IS transfer]-> (a2 IS account)
  WHERE NVL(JSON_VALUE(p.profile, '$.compliance.pep' RETURNING VARCHAR2(5) NULL ON EMPTY NULL ON ERROR), 'false') = 'true'
     OR NVL(JSON_VALUE(e.event_json, '$.aml.score' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR), 0) >= 90
  COLUMNS (
    VERTEX_ID(a1) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(a1) AS source_vertex_id,
    VERTEX_ID(a2) AS destination_vertex_id,
    p.first_name AS person_name,
    a1.account_number AS src_account,
    a2.account_number AS dst_account,
    e.amount AS amount,
    NVL(JSON_VALUE(p.profile, '$.compliance.pep' RETURNING VARCHAR2(5) NULL ON EMPTY NULL ON ERROR), 'false') AS pep_flag,
    NVL(JSON_VALUE(e.event_json, '$.aml.scenario' RETURNING VARCHAR2(40) NULL ON EMPTY NULL ON ERROR), 'N/A') AS aml_scenario,
    NVL(JSON_VALUE(e.event_json, '$.aml.score' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR), 0) AS aml_score,
    NVL(JSON_VALUE(e.event_json, '$.counterparty.merchant.mcc' RETURNING VARCHAR2(10) NULL ON EMPTY NULL ON ERROR), 'N/A') AS merchant_mcc
  )
)
ORDER BY aml_score DESC, amount DESC;
