-- ============================================================================
-- RAG Knowledge Graph + JSON Demo (Oracle Property Graph)
-- 场景：模拟“单篇文档”在入库后进行知识点抽取，并构建图用于 RAG 检索
-- 目标：
--   1) 文档 -> 分块(Chunk)
--   2) Chunk -> 概念(Concept)
--   3) 概念之间关系(Concept->Concept)
--   4) 用图查询做“召回 + 解释 + 引用定位”
-- ============================================================================

-- 可重复执行清理
BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH rag_kb_graph'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE rag_edges PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE rag_concepts PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE rag_chunks PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE rag_documents PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- 1) 文档顶点
CREATE TABLE rag_documents (
  doc_id      NUMBER PRIMARY KEY,
  title       VARCHAR2(300) NOT NULL,
  source_uri  VARCHAR2(500),
  published_at TIMESTAMP,
  doc_json    JSON
);

-- 2) Chunk 顶点（模拟切片后的片段）
CREATE TABLE rag_chunks (
  chunk_id     NUMBER PRIMARY KEY,
  doc_id       NUMBER NOT NULL,
  chunk_no     NUMBER NOT NULL,
  chunk_text   CLOB,
  chunk_json   JSON,
  CONSTRAINT fk_rag_chunk_doc FOREIGN KEY (doc_id) REFERENCES rag_documents(doc_id)
);

-- 3) 概念顶点（抽取后的知识点）
CREATE TABLE rag_concepts (
  concept_id    NUMBER PRIMARY KEY,
  concept_name  VARCHAR2(200) NOT NULL,
  concept_type  VARCHAR2(80) NOT NULL,
  concept_json  JSON
);

-- 4) 统一边表（多类型边）
CREATE TABLE rag_edges (
  edge_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  src_doc_id       NUMBER,
  dst_chunk_id     NUMBER,
  src_chunk_id     NUMBER,
  dst_concept_id   NUMBER,
  src_concept_id   NUMBER,
  dst_concept2_id  NUMBER,
  src_chunk2_id    NUMBER,
  dst_chunk2_id    NUMBER,
  edge_type        VARCHAR2(80) NOT NULL,
  edge_time        TIMESTAMP,
  edge_json        JSON,
  CONSTRAINT fk_edge_src_doc      FOREIGN KEY (src_doc_id)      REFERENCES rag_documents(doc_id),
  CONSTRAINT fk_edge_dst_chunk    FOREIGN KEY (dst_chunk_id)    REFERENCES rag_chunks(chunk_id),
  CONSTRAINT fk_edge_src_chunk    FOREIGN KEY (src_chunk_id)    REFERENCES rag_chunks(chunk_id),
  CONSTRAINT fk_edge_dst_concept  FOREIGN KEY (dst_concept_id)  REFERENCES rag_concepts(concept_id),
  CONSTRAINT fk_edge_src_concept  FOREIGN KEY (src_concept_id)  REFERENCES rag_concepts(concept_id),
  CONSTRAINT fk_edge_dst_concept2 FOREIGN KEY (dst_concept2_id) REFERENCES rag_concepts(concept_id),
  CONSTRAINT fk_edge_src_chunk2   FOREIGN KEY (src_chunk2_id)   REFERENCES rag_chunks(chunk_id),
  CONSTRAINT fk_edge_dst_chunk2   FOREIGN KEY (dst_chunk2_id)   REFERENCES rag_chunks(chunk_id)
);


-- ============================================================================
-- 5) 模拟“单篇文档”与抽取结果
-- ============================================================================

INSERT INTO rag_documents VALUES (
  1,
  'Oracle Graph + JSON for RAG Search',
  'kb://docs/oracle-graph-json-rag',
  TIMESTAMP '2026-02-12 10:00:00',
  '{
     "lang":"zh-CN",
     "domain":"enterprise-rag",
     "tags":["rag","oracle","graph","json"],
     "quality":{"score":96},
     "owner":"Knowledge Engineering"
   }'
);

-- 文档切块（5段）
INSERT INTO rag_chunks VALUES (
  101, 1, 1,
  'RAG系统中，文档需要先切块并构建可检索结构，避免仅靠向量召回导致上下文不足。',
  '{"token_count":48,"embedding_model":"text-embedding-3-large","keywords":["RAG","切块","检索"],"section":"intro"}'
);

INSERT INTO rag_chunks VALUES (
  102, 1, 2,
  'Property Graph可把文档、Chunk、概念、引用关系统一建模，支持多跳语义检索。',
  '{"token_count":52,"embedding_model":"text-embedding-3-large","keywords":["Property Graph","多跳","语义检索"],"section":"modeling"}'
);

INSERT INTO rag_chunks VALUES (
  103, 1, 3,
  'JSON字段可以承载非固定属性，如置信度、证据、标签与抽取策略版本。',
  '{"token_count":44,"embedding_model":"text-embedding-3-large","keywords":["JSON","置信度","标签"],"section":"json-flex"}'
);

INSERT INTO rag_chunks VALUES (
  104, 1, 4,
  '在回答阶段可通过图追踪证据链，从答案概念回溯到Chunk和原文档，提升可解释性。',
  '{"token_count":50,"embedding_model":"text-embedding-3-large","keywords":["证据链","可解释性","回溯"],"section":"grounding"}'
);

INSERT INTO rag_chunks VALUES (
  105, 1, 5,
  'Graph Visualization要求查询结果包含vertex_id和edge_id，便于在SQL Developer直接可视化。',
  '{"token_count":46,"embedding_model":"text-embedding-3-large","keywords":["Graph Visualization","vertex_id","edge_id"],"section":"ops"}'
);

-- 抽取出的概念
INSERT INTO rag_concepts VALUES (201, 'RAG Retrieval', 'METHOD',
  '{"aliases":["retrieval-augmented generation"],"importance":0.95,"topic":"rag"}');
INSERT INTO rag_concepts VALUES (202, 'Document Chunking', 'METHOD',
  '{"aliases":["text chunking"],"importance":0.93,"topic":"pipeline"}');
INSERT INTO rag_concepts VALUES (203, 'Property Graph', 'TECH',
  '{"aliases":["graph model"],"importance":0.97,"topic":"graph"}');
INSERT INTO rag_concepts VALUES (204, 'JSON Flexible Schema', 'TECH',
  '{"aliases":["semi-structured"],"importance":0.90,"topic":"json"}');
INSERT INTO rag_concepts VALUES (205, 'Evidence Grounding', 'QUALITY',
  '{"aliases":["citation grounding"],"importance":0.96,"topic":"trust"}');
INSERT INTO rag_concepts VALUES (206, 'Graph Visualization', 'TOOLING',
  '{"aliases":["SQL Developer Graph"],"importance":0.88,"topic":"ops"}');


-- ============================================================================
-- 6) 边数据：文档结构、概念抽取、概念关系、Chunk顺序
-- ============================================================================

-- DOC -> CHUNK (HAS_CHUNK)
INSERT INTO rag_edges (src_doc_id, dst_chunk_id, edge_type, edge_time, edge_json) VALUES
  (1, 101, 'HAS_CHUNK', TIMESTAMP '2026-02-12 10:01:00', '{"order":1,"strategy":"semantic-split"}');
INSERT INTO rag_edges (src_doc_id, dst_chunk_id, edge_type, edge_time, edge_json) VALUES
  (1, 102, 'HAS_CHUNK', TIMESTAMP '2026-02-12 10:01:00', '{"order":2,"strategy":"semantic-split"}');
INSERT INTO rag_edges (src_doc_id, dst_chunk_id, edge_type, edge_time, edge_json) VALUES
  (1, 103, 'HAS_CHUNK', TIMESTAMP '2026-02-12 10:01:00', '{"order":3,"strategy":"semantic-split"}');
INSERT INTO rag_edges (src_doc_id, dst_chunk_id, edge_type, edge_time, edge_json) VALUES
  (1, 104, 'HAS_CHUNK', TIMESTAMP '2026-02-12 10:01:00', '{"order":4,"strategy":"semantic-split"}');
INSERT INTO rag_edges (src_doc_id, dst_chunk_id, edge_type, edge_time, edge_json) VALUES
  (1, 105, 'HAS_CHUNK', TIMESTAMP '2026-02-12 10:01:00', '{"order":5,"strategy":"semantic-split"}');

-- CHUNK -> CONCEPT (MENTIONS)
INSERT INTO rag_edges (src_chunk_id, dst_concept_id, edge_type, edge_time, edge_json) VALUES
  (101, 201, 'MENTIONS', TIMESTAMP '2026-02-12 10:03:00', '{"confidence":0.96,"extractor":"kg-v2"}');
INSERT INTO rag_edges (src_chunk_id, dst_concept_id, edge_type, edge_time, edge_json) VALUES
  (101, 202, 'MENTIONS', TIMESTAMP '2026-02-12 10:03:00', '{"confidence":0.91,"extractor":"kg-v2"}');
INSERT INTO rag_edges (src_chunk_id, dst_concept_id, edge_type, edge_time, edge_json) VALUES
  (102, 203, 'MENTIONS', TIMESTAMP '2026-02-12 10:04:00', '{"confidence":0.97,"extractor":"kg-v2"}');
INSERT INTO rag_edges (src_chunk_id, dst_concept_id, edge_type, edge_time, edge_json) VALUES
  (102, 201, 'MENTIONS', TIMESTAMP '2026-02-12 10:04:00', '{"confidence":0.88,"extractor":"kg-v2"}');
INSERT INTO rag_edges (src_chunk_id, dst_concept_id, edge_type, edge_time, edge_json) VALUES
  (103, 204, 'MENTIONS', TIMESTAMP '2026-02-12 10:05:00', '{"confidence":0.95,"extractor":"kg-v2"}');
INSERT INTO rag_edges (src_chunk_id, dst_concept_id, edge_type, edge_time, edge_json) VALUES
  (104, 205, 'MENTIONS', TIMESTAMP '2026-02-12 10:06:00', '{"confidence":0.98,"extractor":"kg-v2"}');
INSERT INTO rag_edges (src_chunk_id, dst_concept_id, edge_type, edge_time, edge_json) VALUES
  (105, 206, 'MENTIONS', TIMESTAMP '2026-02-12 10:07:00', '{"confidence":0.99,"extractor":"kg-v2"}');
INSERT INTO rag_edges (src_chunk_id, dst_concept_id, edge_type, edge_time, edge_json) VALUES
  (105, 203, 'MENTIONS', TIMESTAMP '2026-02-12 10:07:00', '{"confidence":0.85,"extractor":"kg-v2"}');

-- CONCEPT -> CONCEPT (RELATED_TO)
INSERT INTO rag_edges (src_concept_id, dst_concept2_id, edge_type, edge_time, edge_json) VALUES
  (202, 201, 'RELATED_TO', TIMESTAMP '2026-02-12 10:09:00', '{"weight":0.90,"reason":"chunking-improves-retrieval"}');
INSERT INTO rag_edges (src_concept_id, dst_concept2_id, edge_type, edge_time, edge_json) VALUES
  (203, 201, 'RELATED_TO', TIMESTAMP '2026-02-12 10:09:10', '{"weight":0.92,"reason":"graph-retrieval"}');
INSERT INTO rag_edges (src_concept_id, dst_concept2_id, edge_type, edge_time, edge_json) VALUES
  (204, 203, 'RELATED_TO', TIMESTAMP '2026-02-12 10:09:20', '{"weight":0.87,"reason":"json-property-modeling"}');
INSERT INTO rag_edges (src_concept_id, dst_concept2_id, edge_type, edge_time, edge_json) VALUES
  (205, 201, 'RELATED_TO', TIMESTAMP '2026-02-12 10:09:30', '{"weight":0.93,"reason":"grounded-answer-quality"}');
INSERT INTO rag_edges (src_concept_id, dst_concept2_id, edge_type, edge_time, edge_json) VALUES
  (206, 203, 'RELATED_TO', TIMESTAMP '2026-02-12 10:09:40', '{"weight":0.86,"reason":"graph-ops"}');

-- CHUNK -> CHUNK (NEXT) 体现上下文顺序
INSERT INTO rag_edges (src_chunk2_id, dst_chunk2_id, edge_type, edge_time, edge_json) VALUES
  (101, 102, 'NEXT', TIMESTAMP '2026-02-12 10:02:00', '{"distance":1}');
INSERT INTO rag_edges (src_chunk2_id, dst_chunk2_id, edge_type, edge_time, edge_json) VALUES
  (102, 103, 'NEXT', TIMESTAMP '2026-02-12 10:02:10', '{"distance":1}');
INSERT INTO rag_edges (src_chunk2_id, dst_chunk2_id, edge_type, edge_time, edge_json) VALUES
  (103, 104, 'NEXT', TIMESTAMP '2026-02-12 10:02:20', '{"distance":1}');
INSERT INTO rag_edges (src_chunk2_id, dst_chunk2_id, edge_type, edge_time, edge_json) VALUES
  (104, 105, 'NEXT', TIMESTAMP '2026-02-12 10:02:30', '{"distance":1}');

COMMIT;


-- ============================================================================
-- 7) Property Graph 定义
-- ============================================================================
CREATE PROPERTY GRAPH rag_kb_graph
  VERTEX TABLES (
    rag_documents AS document
      KEY (doc_id)
      PROPERTIES (doc_id, title, source_uri, published_at, doc_json),

    rag_chunks AS chunk
      KEY (chunk_id)
      PROPERTIES (chunk_id, doc_id, chunk_no, chunk_text, chunk_json),

    rag_concepts AS concept
      KEY (concept_id)
      PROPERTIES (concept_id, concept_name, concept_type, concept_json)
  )
  EDGE TABLES (
    rag_edges AS has_chunk
      KEY (edge_id)
      SOURCE KEY (src_doc_id) REFERENCES document (doc_id)
      DESTINATION KEY (dst_chunk_id) REFERENCES chunk (chunk_id)
      LABEL has_chunk
      PROPERTIES (edge_id, edge_type, edge_time, edge_json),

    rag_edges AS mentions
      KEY (edge_id)
      SOURCE KEY (src_chunk_id) REFERENCES chunk (chunk_id)
      DESTINATION KEY (dst_concept_id) REFERENCES concept (concept_id)
      LABEL mentions
      PROPERTIES (edge_id, edge_type, edge_time, edge_json),

    rag_edges AS related_to
      KEY (edge_id)
      SOURCE KEY (src_concept_id) REFERENCES concept (concept_id)
      DESTINATION KEY (dst_concept2_id) REFERENCES concept (concept_id)
      LABEL related_to
      PROPERTIES (edge_id, edge_type, edge_time, edge_json),

    rag_edges AS next_chunk
      KEY (edge_id)
      SOURCE KEY (src_chunk2_id) REFERENCES chunk (chunk_id)
      DESTINATION KEY (dst_chunk2_id) REFERENCES chunk (chunk_id)
      LABEL next_chunk
      PROPERTIES (edge_id, edge_type, edge_time, edge_json)
  );


-- ============================================================================
-- 8) 查询示例 A：抽取图全貌（可直接 Graph Visualization）
-- ============================================================================
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  src_kind,
  dst_kind,
  edge_type,
  score
FROM GRAPH_TABLE (
  rag_kb_graph
  MATCH (s) -[e]-> (t)
  COLUMNS (
    VERTEX_ID(s) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(s) AS source_vertex_id,
    VERTEX_ID(t) AS destination_vertex_id,
    CASE
      WHEN s.doc_id IS NOT NULL THEN 'DOCUMENT'
      WHEN s.chunk_id IS NOT NULL THEN 'CHUNK'
      ELSE 'CONCEPT'
    END AS src_kind,
    CASE
      WHEN t.doc_id IS NOT NULL THEN 'DOCUMENT'
      WHEN t.chunk_id IS NOT NULL THEN 'CHUNK'
      ELSE 'CONCEPT'
    END AS dst_kind,
    e.edge_type AS edge_type,
    NVL(JSON_VALUE(e.edge_json, '$.confidence' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR),
      NVL(JSON_VALUE(e.edge_json, '$.weight' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR), 0)) AS score
  )
)
ORDER BY edge_type, score DESC;


-- ============================================================================
-- 9) 查询示例 B：RAG 检索召回（按“问题概念”扩散到相关Chunk）
-- 模拟问题："如何提升RAG答案可解释性？"
-- 思路：从 Evidence Grounding 概念出发，找相关概念 + 被提及的Chunk
-- ============================================================================
SELECT
  query_concept,
  related_concept,
  chunk_no,
  chunk_text,
  related_weight,
  mention_confidence
FROM GRAPH_TABLE (
  rag_kb_graph
  MATCH (c0 IS concept) -[r IS related_to]-> (c1 IS concept) <-[m IS mentions]- (ch IS chunk)
  WHERE c0.concept_name = 'Evidence Grounding'
  COLUMNS (
    c0.concept_name AS query_concept,
    c1.concept_name AS related_concept,
    ch.chunk_no AS chunk_no,
    ch.chunk_text AS chunk_text,
    NVL(JSON_VALUE(r.edge_json, '$.weight' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR), 0) AS related_weight,
    NVL(JSON_VALUE(m.edge_json, '$.confidence' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR), 0) AS mention_confidence
  )
)
ORDER BY related_weight DESC, mention_confidence DESC;


-- ============================================================================
-- 10) 查询示例 C：答案溯源（概念 -> Chunk -> 文档）
-- 用于RAG回答时给出可解释引用
-- ============================================================================
SELECT
  concept,
  doc_title,
  doc_source,
  chunk_no,
  chunk_text,
  extract_confidence,
  doc_quality
FROM GRAPH_TABLE (
  rag_kb_graph
  MATCH (d IS document) -[h IS has_chunk]-> (ch IS chunk) -[m IS mentions]-> (c IS concept)
  WHERE c.concept_name IN ('Property Graph', 'JSON Flexible Schema', 'Evidence Grounding')
  COLUMNS (
    c.concept_name AS concept,
    d.title AS doc_title,
    d.source_uri AS doc_source,
    ch.chunk_no AS chunk_no,
    ch.chunk_text AS chunk_text,
    NVL(JSON_VALUE(m.edge_json, '$.confidence' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR), 0) AS extract_confidence,
    NVL(JSON_VALUE(d.doc_json, '$.quality.score' RETURNING NUMBER NULL ON EMPTY NULL ON ERROR), 0) AS doc_quality
  )
)
ORDER BY concept, extract_confidence DESC;
