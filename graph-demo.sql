CREATE TABLE persons (
  person_id NUMBER PRIMARY KEY,
  first_name VARCHAR2(200),
  last_name VARCHAR2(200),
  more_data JSON
);

INSERT INTO persons VALUES (
  1, 'Camille', 'Nichols', '{"address":{"address_line_1":"101 Ellison Avenue",
                                        "zip_code":10001,
                                        "city":"Larry Islands",
                                        "state":"AL"}}');
INSERT INTO persons VALUES (
  2, 'Jake', 'Zimmerman', '{"address":{"address_line_1":"500 Property Graph Parkway",
                                       "zip_code":75002,
                                       "city":"Oracle City",
                                       "state":"AK"}}');
INSERT INTO persons VALUES (
  3, 'Nikita', 'Ivanov', '{"address":{"address_line_1":"1 Boolean Boulevard",
                                      "zip_code":48201,
                                      "city":"Vectorville",
                                      "state":"AZ"}}');
INSERT INTO persons VALUES (
  4, 'Liam', 'O''Connor', '{"address":{"address_line_1":"16 SQL Street",
                                       "zip_code":9075,
                                       "city":"Json Town",
                                       "state":"CA"}}');

CREATE TABLE accounts (
  account_number NUMBER PRIMARY KEY,
  owner_id NUMBER,
  creation_date TIMESTAMP,
  FOREIGN KEY (owner_id) REFERENCES persons (person_id)
);

INSERT INTO accounts VALUES (1001, 2, TIMESTAMP '2000-01-01 14:31:00');
INSERT INTO accounts VALUES (2090, 4, TIMESTAMP '2004-12-15 08:15:00');
INSERT INTO accounts VALUES (8021, 3, TIMESTAMP '2005-03-20 10:45:00');
INSERT INTO accounts VALUES (10039, 1, TIMESTAMP '2020-12-15 14:17:00');

CREATE TABLE transfers (
  transfer_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_account NUMBER,
  to_account NUMBER,
  time TIMESTAMP,
  amount NUMBER(10,2)
);

INSERT INTO transfers (from_account, to_account, time, amount)
  VALUES (10039, 8021, TIMESTAMP '2024-03-12 14:30:00', 1000.00);
INSERT INTO transfers (from_account, to_account, time, amount)
  VALUES (8021, 1001, TIMESTAMP '2024-03-12 16:30:00', 1500.30);
INSERT INTO transfers (from_account, to_account, time, amount)
  VALUES (8021, 1001, TIMESTAMP '2024-03-12 16:31:00', 3000.00);
INSERT INTO transfers (from_account, to_account, time, amount)
  VALUES (1001, 2090, TIMESTAMP '2024-03-12 19:03:00', 9999.50);
INSERT INTO transfers (from_account, to_account, time, amount)
  VALUES (2090, 10039, TIMESTAMP '2024-03-12 19:45:00', 9900.00);



CREATE PROPERTY GRAPH bank_graph
  VERTEX TABLES (
    persons AS person,
    accounts AS account
  )
  EDGE TABLES (
    transfers AS transfer
      SOURCE KEY (from_account) REFERENCES account (account_number)
      DESTINATION KEY (to_account) REFERENCES account (account_number)
      PROPERTIES (transfer_id, time, amount),
    accounts AS owner
      SOURCE KEY (account_number) REFERENCES account (account_number)
      DESTINATION person
      NO PROPERTIES
  );

-- Graph Visualization 需要在 COLUMNS 中包含 vertex_id / edge_id，
-- 并在外层 SELECT 中显式投影这些列名。
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  from_account,
  to_account,
  amount,
  transfer_time
FROM GRAPH_TABLE ( bank_graph
       MATCH (a1 IS account) -[e IS transfer]-> (a2 IS account)
       COLUMNS (
         VERTEX_ID(a1) AS vertex_id,
         EDGE_ID(e) AS edge_id,
         VERTEX_ID(a1) AS source_vertex_id,
         VERTEX_ID(a2) AS destination_vertex_id,
         a1.account_number AS from_account,
         a2.account_number AS to_account,
         e.amount AS amount,
         e.time AS transfer_time
       )
);

-- 注意：下面这类聚合/路径分析查询不适合直接用于 Graph Visualization，
-- 若需要使用图可视化，请仅运行上面的可视化查询（包含 vertex_id / edge_id）。
--
-- SELECT *
-- FROM GRAPH_TABLE ( bank_graph
--        MATCH (p1 IS person) <-[IS owner]- (a1 IS account),
--              (a1) -[e IS transfer]->{1,2} (a2 IS account),
--              (a2) -[IS owner]-> (p2 IS person)
--        WHERE p1.first_name = 'Camille'
--        COLUMNS (p2.first_name, p2.last_name,
--                 p2.more_data.address.address_line_1.string() AS street,
--                 COUNT(e.transfer_id) AS path_length,
--                 JSON_ARRAYAGG(e.amount) AS amounts))
-- ORDER BY path_length, amounts;