-- =============================================================================
-- Oracle 26ai Property Graph Demo: 电影推荐场景（仅基于 normalized_movies.csv 导入）
-- 说明：
--   1) 不再手工写入样例业务数据
--   2) 所有电影数据都来源于 normalized_movies.csv -> movies_pg_stage -> movies_pg
--   3) 图中的边由导入后的电影数据自动派生生成，而非手工写死
-- =============================================================================

BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH movie_graph_pg'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_same_collection_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_same_language_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movies_pg PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movies_pg_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- 1) Staging 表：用于直接承接 normalized_movies.csv 原始字段
CREATE TABLE movies_pg_stage (
  tmdbId                VARCHAR2(50),
  original_title        VARCHAR2(500),
  adult                 VARCHAR2(10),
  budget                VARCHAR2(50),
  imdb_id               VARCHAR2(50),
  original_language     VARCHAR2(20),
  revenue               VARCHAR2(50),
  tagline               VARCHAR2(1000),
  title                 VARCHAR2(500),
  release_date          VARCHAR2(50),
  runtime               VARCHAR2(50),
  overview              CLOB,
  belongs_to_collection VARCHAR2(500)
) ANNOTATIONS (
  Description 'normalized_movies.csv 的 staging 表：先按文本接收，再转换装载到 movies_pg'
);

-- 真实导入命令示例（按环境选择其一）
-- [方式 A] SQL Developer 图形界面导入
--   1. 先执行本脚本，创建 movies_pg_stage
--   2. 在 SQL Developer 中右键 MOVIES_PG_STAGE -> Import Data
--   3. 选择 normalized_movies.csv
--   4. 勾选“第一行是表头”并完成列映射
--
-- [方式 B] SQLcl 内置 load 命令
--   SQL> set sqlformat csv
--   SQL> cd /path/to/csv-files
--   SQL> truncate table movies_pg_stage;
--   SQL> load movies_pg_stage normalized_movies.csv
--   SQL> select count(*) from movies_pg_stage;
--   SQL> select * from movies_pg_stage fetch first 5 rows only;
--
-- [方式 C] SQL*Loader
--   先准备控制文件 movies_pg_stage.ctl：
--
--   LOAD DATA
--   INFILE 'normalized_movies.csv'
--   INTO TABLE movies_pg_stage
--   APPEND
--   FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
--   TRAILING NULLCOLS
--   (
--     tmdbId,
--     original_title,
--     adult,
--     budget,
--     imdb_id,
--     original_language,
--     revenue,
--     tagline,
--     title,
--     release_date,
--     runtime,
--     overview CHAR(4000),
--     belongs_to_collection
--   )
--
--   执行命令：
--   sqlldr your_user/your_password@your_service control=movies_pg_stage.ctl log=movies_pg_stage.log skip=1
--
-- [方式 D] DBMS_CLOUD.COPY_DATA（对象存储 / HTTPS）
--   BEGIN
--     DBMS_CLOUD.CREATE_CREDENTIAL(
--       credential_name => 'MOVIE_CRED',
--       username        => 'your_username',
--       password        => 'your_token_or_auth'
--     );
--   END;
--   /
--
--   BEGIN
--     DBMS_CLOUD.COPY_DATA(
--       table_name      => 'MOVIES_PG_STAGE',
--       credential_name => 'MOVIE_CRED',
--       file_uri_list   => 'https://your-bucket/path/normalized_movies.csv',
--       format          => json_object(
--         'type' value 'csv',
--         'skipheaders' value 1,
--         'delimiter' value ',',
--         'quote' value '"'
--       )
--     );
--   END;
--   /

-- 2) Movie 顶点表：由 staging 表转换生成
CREATE TABLE movies_pg (
  tmdb_id             NUMBER PRIMARY KEY,
  movie_id            NUMBER UNIQUE,
  title               VARCHAR2(300),
  original_title      VARCHAR2(300),
  adult_flag          VARCHAR2(3),
  budget              NUMBER,
  revenue             NUMBER,
  imdb_id             VARCHAR2(20),
  original_language   VARCHAR2(20),
  release_date        DATE,
  runtime_minutes     NUMBER,
  movie_json          JSON
) ANNOTATIONS (
  Description '电影顶点表：由 normalized_movies.csv 清洗转换后生成的 Movie 顶点'
);

-- 将 staging 数据装载到正式表
INSERT INTO movies_pg (
  tmdb_id,
  movie_id,
  title,
  original_title,
  adult_flag,
  budget,
  revenue,
  imdb_id,
  original_language,
  release_date,
  runtime_minutes,
  movie_json
)
SELECT
  TO_NUMBER(tmdbId) AS tmdb_id,
  ROW_NUMBER() OVER (ORDER BY TO_NUMBER(tmdbId)) AS movie_id,
  title,
  original_title,
  CASE WHEN NVL(adult, '0') IN ('1', 'true', 'TRUE') THEN 'Yes' ELSE 'No' END AS adult_flag,
  TO_NUMBER(NVL(NULLIF(budget, ''), '0')) AS budget,
  TO_NUMBER(NVL(NULLIF(revenue, ''), '0')) AS revenue,
  imdb_id,
  original_language,
  CASE
    WHEN release_date IS NOT NULL AND release_date <> '' THEN TO_DATE(release_date, 'YYYY-MM-DD')
    ELSE NULL
  END AS release_date,
  TO_NUMBER(NVL(NULLIF(REPLACE(runtime, '.0', ''), ''), '0')) AS runtime_minutes,
  JSON_OBJECT(
    'tagline' VALUE NVL(tagline, ''),
    'overview' VALUE NVL(overview, ''),
    'belongs_to_collection' VALUE NVL(belongs_to_collection, 'None')
  ) AS movie_json
FROM movies_pg_stage
WHERE tmdbId IS NOT NULL;

-- 3) 派生边表：全部来自已导入的电影数据

-- 3.1 同系列电影关系：belongs_to_collection 相同且不为 None
CREATE TABLE movie_same_collection_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  src_tmdb_id         NUMBER NOT NULL,
  dst_tmdb_id         NUMBER NOT NULL,
  relation_name       VARCHAR2(50),
  rel_json            JSON,
  CONSTRAINT fk_msc_src FOREIGN KEY (src_tmdb_id) REFERENCES movies_pg(tmdb_id),
  CONSTRAINT fk_msc_dst FOREIGN KEY (dst_tmdb_id) REFERENCES movies_pg(tmdb_id)
) ANNOTATIONS (
  Description '电影之间的同系列关系：由 belongs_to_collection 自动派生'
);

INSERT INTO movie_same_collection_rel (src_tmdb_id, dst_tmdb_id, relation_name, rel_json)
SELECT
  m1.tmdb_id,
  m2.tmdb_id,
  'SAME_COLLECTION',
  JSON_OBJECT(
    'collection' VALUE JSON_VALUE(m1.movie_json, '$.belongs_to_collection' RETURNING VARCHAR2(500))
  )
FROM movies_pg m1
JOIN movies_pg m2
  ON m1.tmdb_id < m2.tmdb_id
 AND JSON_VALUE(m1.movie_json, '$.belongs_to_collection' RETURNING VARCHAR2(500)) =
     JSON_VALUE(m2.movie_json, '$.belongs_to_collection' RETURNING VARCHAR2(500))
WHERE JSON_VALUE(m1.movie_json, '$.belongs_to_collection' RETURNING VARCHAR2(500)) <> 'None';

-- 3.2 同语言关系：original_language 相同
CREATE TABLE movie_same_language_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  src_tmdb_id         NUMBER NOT NULL,
  dst_tmdb_id         NUMBER NOT NULL,
  relation_name       VARCHAR2(50),
  rel_json            JSON,
  CONSTRAINT fk_msl_src FOREIGN KEY (src_tmdb_id) REFERENCES movies_pg(tmdb_id),
  CONSTRAINT fk_msl_dst FOREIGN KEY (dst_tmdb_id) REFERENCES movies_pg(tmdb_id)
) ANNOTATIONS (
  Description '电影之间的同语言关系：由 original_language 自动派生'
);

INSERT INTO movie_same_language_rel (src_tmdb_id, dst_tmdb_id, relation_name, rel_json)
SELECT
  m1.tmdb_id,
  m2.tmdb_id,
  'SAME_LANGUAGE',
  JSON_OBJECT('language' VALUE m1.original_language)
FROM movies_pg m1
JOIN movies_pg m2
  ON m1.tmdb_id < m2.tmdb_id
 AND m1.original_language = m2.original_language;

COMMIT;

-- 4) Property Graph 定义
CREATE PROPERTY GRAPH movie_graph_pg
  VERTEX TABLES (
    movies_pg AS movie
      KEY (tmdb_id)
      PROPERTIES (
        tmdb_id, movie_id, title, original_title, adult_flag, budget, revenue,
        imdb_id, original_language, release_date, runtime_minutes, movie_json
      )
  )
  EDGE TABLES (
    movie_same_collection_rel AS same_collection
      KEY (rel_id)
      SOURCE KEY (src_tmdb_id) REFERENCES movie (tmdb_id)
      DESTINATION KEY (dst_tmdb_id) REFERENCES movie (tmdb_id)
      PROPERTIES (rel_id, relation_name, rel_json),
    movie_same_language_rel AS same_language
      KEY (rel_id)
      SOURCE KEY (src_tmdb_id) REFERENCES movie (tmdb_id)
      DESTINATION KEY (dst_tmdb_id) REFERENCES movie (tmdb_id)
      PROPERTIES (rel_id, relation_name, rel_json)
  );

-- 5) 可视化查询 A：同系列电影
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  src_movie,
  dst_movie,
  collection_name
FROM GRAPH_TABLE (
  movie_graph_pg
  MATCH (m1 IS movie) -[e IS same_collection]-> (m2 IS movie)
  COLUMNS (
    VERTEX_ID(m1) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(m1) AS source_vertex_id,
    VERTEX_ID(m2) AS destination_vertex_id,
    m1.title AS src_movie,
    m2.title AS dst_movie,
    JSON_VALUE(e.rel_json, '$.collection' RETURNING VARCHAR2(500)) AS collection_name
  )
)
ORDER BY collection_name, src_movie, dst_movie;

-- 6) 可视化查询 B：同语言电影
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  src_movie,
  dst_movie,
  language_code
FROM GRAPH_TABLE (
  movie_graph_pg
  MATCH (m1 IS movie) -[e IS same_language]-> (m2 IS movie)
  COLUMNS (
    VERTEX_ID(m1) AS vertex_id,
    EDGE_ID(e) AS edge_id,
    VERTEX_ID(m1) AS source_vertex_id,
    VERTEX_ID(m2) AS destination_vertex_id,
    m1.title AS src_movie,
    m2.title AS dst_movie,
    JSON_VALUE(e.rel_json, '$.language' RETURNING VARCHAR2(20)) AS language_code
  )
)
ORDER BY language_code, src_movie, dst_movie;

-- 7) 分析查询：以 Toy Story 为起点发现同语言电影
SELECT
  base_movie,
  related_movie,
  related_revenue,
  related_runtime
FROM GRAPH_TABLE (
  movie_graph_pg
  MATCH (m1 IS movie) -[e IS same_language]-> (m2 IS movie)
  WHERE m1.title = 'Toy Story'
  COLUMNS (
    m1.title AS base_movie,
    m2.title AS related_movie,
    m2.revenue AS related_revenue,
    m2.runtime_minutes AS related_runtime
  )
)
ORDER BY related_revenue DESC, related_runtime DESC;

-- 使用说明：
-- 1. 执行本脚本创建 staging 表与正式表
-- 2. 用 SQLcl / SQL Developer / SQL*Loader / DBMS_CLOUD 把 normalized_movies.csv 导入 MOVIES_PG_STAGE
-- 3. 执行本脚本中的 movies_pg 装载 SQL与派生边生成 SQL
-- 4. 执行 CREATE PROPERTY GRAPH movie_graph_pg
-- 5. 运行第 5/6/7 段图查询
