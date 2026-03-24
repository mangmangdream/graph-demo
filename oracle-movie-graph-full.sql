-- =============================================================================
-- Oracle 26ai Property Graph Demo: 完整电影推荐图谱（对齐 graph_build.py）
--
-- 数据来源与 graph_build.py 保持一致：
--   normalized_movies.csv
--   normalized_genres.csv
--   normalized_production_companies.csv
--   normalized_production_countries.csv
--   normalized_spoken_languages.csv
--   normalized_keywords.csv
--   movie_embeddings.csv
--   normalized_cast.csv
--   normalized_crew.csv
--   normalized_links.csv
--   normalized_ratings_small.csv
--
-- 说明：
--   1) 本脚本按“多 CSV -> staging 表 -> 正式表 -> Property Graph”方式组织。
--   2) 表结构已按你提供的各 CSV 字段校正。
--   3) 为便于理解，所有正式表与主要 staging 表均补充了 ANNOTATIONS 说明。
--
-- 关系对应说明（对照 Neo4j Relationship Counts）：
--   HAS_GENRE   -> movie_genre_rel            (Movie -> Genre)
--   PRODUCED_BY -> movie_production_rel       (Movie -> ProductionCompany)
--   PRODUCED_IN -> movie_release_country_rel  (Movie -> Country)
--   HAS_LANGUAGE-> movie_language_rel         (Movie -> SpokenLanguage)
--   ACTED_IN    -> movie_cast_rel             (Person(actor) -> Movie)
--   DIRECTED    -> movie_crew_rel             (job_name = 'Director')
--   PRODUCED    -> movie_crew_rel             (job_name = 'Producer')
--   RATED       -> user_rating_rel            (User -> Movie)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0) 清理对象
-- -----------------------------------------------------------------------------
BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH movie_graph_full_pg'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_release_country_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP INDEX idx_rating_user_score_movie'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP INDEX idx_rating_movie_user'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP INDEX idx_genre_movie'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP INDEX idx_genre_genre_movie'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP INDEX idx_cast_movie_person'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP INDEX idx_crew_movie_person'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_language_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE user_rating_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_production_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_genre_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_crew_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_cast_rel PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE countries_pg PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE spoken_languages_pg PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE users_pg PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE production_companies_pg PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE genres_pg PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE persons_pg PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movies_pg PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

BEGIN EXECUTE IMMEDIATE 'DROP TABLE movies_pg_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE genres_pg_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE production_companies_pg_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE countries_pg_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE spoken_languages_pg_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_keywords_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_embeddings_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_cast_rel_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_crew_rel_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE movie_links_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE user_rating_rel_stage PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- -----------------------------------------------------------------------------
-- 1) staging 表：字段严格对应各个 CSV
-- -----------------------------------------------------------------------------
CREATE TABLE movies_pg_stage (
  tmdbId                VARCHAR2(50),
  original_title        VARCHAR2(500),
  adult                 VARCHAR2(10),
  budget                VARCHAR2(50),
  imdb_id               VARCHAR2(50),
  original_language     VARCHAR2(20),
  revenue               VARCHAR2(50),
  tagline               VARCHAR2(2000),
  title                 VARCHAR2(500),
  release_date          VARCHAR2(50),
  runtime               VARCHAR2(50),
  overview              CLOB,
  belongs_to_collection VARCHAR2(500)
) ANNOTATIONS (
  Description 'normalized_movies.csv 的 staging 表；字段与 CSV 完全对应，用于装载电影主数据。',
  tmdbId 'TMDB 电影标识',
  original_title '电影原始标题',
  adult '成人影片标记，通常 0/1',
  budget '电影预算，原始文本格式',
  imdb_id 'IMDb 标识',
  original_language '电影原始语言代码',
  revenue '电影收入，原始文本格式',
  tagline '电影宣传语',
  title '电影展示标题',
  release_date '上映日期，原始文本 YYYY-MM-DD',
  runtime '片长，原始文本格式',
  overview '电影简介',
  belongs_to_collection '所属系列/合集名称'
);

CREATE TABLE genres_pg_stage (
  genre_id              VARCHAR2(50),
  genre_name            VARCHAR2(100),
  tmdbId                VARCHAR2(50)
) ANNOTATIONS (
  Description 'normalized_genres.csv 的 staging 表；用于装载电影与题材的关联。',
  genre_id '题材 ID',
  genre_name '题材名称',
  tmdbId '关联电影的 tmdbId'
);

CREATE TABLE production_companies_pg_stage (
  company_id            VARCHAR2(50),
  company_name          VARCHAR2(200),
  tmdbId                VARCHAR2(50)
) ANNOTATIONS (
  Description 'normalized_production_companies.csv 的 staging 表；用于装载电影与制片公司的关联。',
  company_id '制片公司 ID',
  company_name '制片公司名称',
  tmdbId '关联电影的 tmdbId'
);

CREATE TABLE countries_pg_stage (
  country_code          VARCHAR2(10),
  country_name          VARCHAR2(100),
  tmdbId                VARCHAR2(50)
) ANNOTATIONS (
  Description 'normalized_production_countries.csv 的 staging 表；用于装载电影与制作/发行国家关联。',
  country_code '国家代码',
  country_name '国家名称',
  tmdbId '关联电影的 tmdbId'
);

CREATE TABLE spoken_languages_pg_stage (
  language_code         VARCHAR2(10),
  language_name         VARCHAR2(100),
  tmdbId                VARCHAR2(50)
) ANNOTATIONS (
  Description 'normalized_spoken_languages.csv 的 staging 表；用于装载电影与语言关联。',
  language_code '语言代码',
  language_name '语言名称',
  tmdbId '关联电影的 tmdbId'
);

CREATE TABLE movie_keywords_stage (
  tmdbId                VARCHAR2(50),
  keywords              CLOB
) ANNOTATIONS (
  Description 'normalized_keywords.csv 的 staging 表；用于把关键词回填为 Movie 属性。',
  tmdbId '关联电影的 tmdbId',
  keywords '电影关键词集合'
);

CREATE TABLE movie_embeddings_stage (
  tmdbId                VARCHAR2(50),
  title                 VARCHAR2(500),
  overview              CLOB,
  embedding             CLOB
) ANNOTATIONS (
  Description 'movie_embeddings.csv 的 staging 表；用于把向量 embedding 回填为 Movie 属性。',
  tmdbId '关联电影的 tmdbId',
  title '电影标题（用于校验/辅助）',
  overview '电影简介（用于校验/辅助）',
  embedding 'JSON 数组格式的 embedding 向量内容'
);

CREATE TABLE movie_cast_rel_stage (
  actor_id              VARCHAR2(50),
  name                  VARCHAR2(200),
  character             VARCHAR2(1000),
  cast_id               VARCHAR2(50),
  tmdbId                VARCHAR2(50)
) ANNOTATIONS (
  Description 'normalized_cast.csv 的 staging 表；用于装载演员与出演关系。',
  actor_id '演员 ID',
  name '演员姓名',
  character '演员饰演的角色名',
  cast_id '演员出演记录 ID',
  tmdbId '关联电影的 tmdbId'
);

CREATE TABLE movie_crew_rel_stage (
  crew_id               VARCHAR2(50),
  name                  VARCHAR2(200),
  job                   VARCHAR2(100),
  tmdbId                VARCHAR2(50)
) ANNOTATIONS (
  Description 'normalized_crew.csv 的 staging 表；用于装载导演/制片人等 crew 关系。',
  crew_id '剧组成员 ID',
  name '剧组成员姓名',
  job '职能，如 Director / Producer',
  tmdbId '关联电影的 tmdbId'
);

CREATE TABLE movie_links_stage (
  movieId               VARCHAR2(50),
  imdbId                VARCHAR2(50),
  tmdbId                VARCHAR2(50)
) ANNOTATIONS (
  Description 'normalized_links.csv 的 staging 表；用于回填 movieId 和 imdbId。',
  movieId '推荐/评分数据中的 movieId',
  imdbId 'IMDb 标识',
  tmdbId 'TMDB 标识，用于和 Movie 对齐'
);

CREATE TABLE user_rating_rel_stage (
  userId                VARCHAR2(50),
  movieId               VARCHAR2(50),
  rating                VARCHAR2(20),
  timestamp             VARCHAR2(50)
) ANNOTATIONS (
  Description 'normalized_ratings_small.csv 的 staging 表；用于装载用户评分关系。',
  userId '用户 ID',
  movieId '评分文件中的 movieId',
  rating '用户评分值',
  timestamp '评分时间戳（Unix 秒）'
);

-- -----------------------------------------------------------------------------
-- 2) SQLcl 导入模板（与 graph_build.py 加载顺序一致）
-- -----------------------------------------------------------------------------
-- SQL> set sqlformat csv
-- SQL> cd /path/to/csv-files
-- SQL> truncate table movies_pg_stage;
-- SQL> truncate table genres_pg_stage;
-- SQL> truncate table production_companies_pg_stage;
-- SQL> truncate table countries_pg_stage;
-- SQL> truncate table spoken_languages_pg_stage;
-- SQL> truncate table movie_keywords_stage;
-- SQL> truncate table movie_embeddings_stage;
-- SQL> truncate table movie_cast_rel_stage;
-- SQL> truncate table movie_crew_rel_stage;
-- SQL> truncate table movie_links_stage;
-- SQL> truncate table user_rating_rel_stage;
--
-- SQL> load movies_pg_stage normalized_movies.csv
-- SQL> load genres_pg_stage normalized_genres.csv
-- SQL> load production_companies_pg_stage normalized_production_companies.csv
-- SQL> load countries_pg_stage normalized_production_countries.csv
-- SQL> load spoken_languages_pg_stage normalized_spoken_languages.csv
-- SQL> load movie_keywords_stage normalized_keywords.csv
-- SQL> load movie_embeddings_stage movie_embeddings.csv
-- SQL> load movie_cast_rel_stage normalized_cast.csv
-- SQL> load movie_crew_rel_stage normalized_crew.csv
-- SQL> load movie_links_stage normalized_links.csv
-- SQL> load user_rating_rel_stage normalized_ratings_small.csv

-- -----------------------------------------------------------------------------
-- 3) 正式顶点表
-- -----------------------------------------------------------------------------
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
  embedding           VECTOR(*, FLOAT32),
  movie_json          JSON
) ANNOTATIONS (
  Description 'Movie 顶点表；由 normalized_movies.csv 与 normalized_links.csv 装载，对应 Neo4j 的 Movie 节点。',
  tmdb_id 'Movie 顶点主键；TMDB 电影标识',
  movie_id '链接/评分体系使用的 movieId',
  title '电影展示标题',
  original_title '电影原始标题',
  adult_flag '成人影片标记，Yes/No',
  budget '电影预算，数值化后字段',
  revenue '电影收入，数值化后字段',
  imdb_id 'IMDb 标识',
  original_language '电影原始语言代码',
  release_date '上映日期',
  runtime_minutes '片长（分钟）',
  embedding '电影向量 embedding；由 movie_embeddings.csv 回填，使用 Oracle VECTOR 类型存储',
  movie_json '电影扩展属性 JSON，如 tagline、overview、belongs_to_collection、keywords'
);

CREATE TABLE persons_pg (
  person_id           NUMBER PRIMARY KEY,
  person_name         VARCHAR2(200),
  role_type           VARCHAR2(30),
  person_json         JSON
) ANNOTATIONS (
  Description 'Person 顶点表；由 normalized_cast.csv 与 normalized_crew.csv 共同装载，统一承载 Actor / Director / Producer / Crew。',
  person_id 'Person 顶点主键；演员或剧组成员标识',
  person_name '人物姓名',
  role_type '人物角色类型，如 ACTOR、DIRECTOR、PRODUCER、CREW',
  person_json '人物扩展属性 JSON'
);

CREATE TABLE genres_pg (
  genre_id            NUMBER PRIMARY KEY,
  genre_name          VARCHAR2(100),
  genre_json          JSON
) ANNOTATIONS (
  Description 'Genre 顶点表；由 normalized_genres.csv 装载。表达电影题材节点，与 Movie 通过 HAS_GENRE/IN_GENRE 关系连接。',
  genre_id 'Genre 顶点主键',
  genre_name '题材名称',
  genre_json '题材扩展属性 JSON'
);

CREATE TABLE production_companies_pg (
  company_id          NUMBER PRIMARY KEY,
  company_name        VARCHAR2(200),
  company_json        JSON
) ANNOTATIONS (
  Description 'ProductionCompany 顶点表；由 normalized_production_companies.csv 装载。表达制片公司节点，与 Movie 通过 PRODUCED_BY 关系连接。',
  company_id '制片公司主键',
  company_name '制片公司名称',
  company_json '制片公司扩展属性 JSON'
);

CREATE TABLE users_pg (
  user_id             NUMBER PRIMARY KEY,
  user_name           VARCHAR2(120),
  user_json           JSON
) ANNOTATIONS (
  Description 'User 顶点表；由 normalized_ratings_small.csv 中的 userId 派生。表达评分用户节点，与 Movie 通过 RATED 关系连接。',
  user_id '用户主键',
  user_name '用户名称；默认由 userId 派生',
  user_json '用户扩展属性 JSON'
);

CREATE TABLE spoken_languages_pg (
  language_code       VARCHAR2(10) PRIMARY KEY,
  language_name       VARCHAR2(100),
  language_json       JSON
) ANNOTATIONS (
  Description 'SpokenLanguage 顶点表；由 normalized_spoken_languages.csv 装载。表达电影语言节点，与 Movie 通过 HAS_LANGUAGE/SPOKEN_IN 关系连接。',
  language_code '语言代码主键',
  language_name '语言名称',
  language_json '语言扩展属性 JSON'
);

CREATE TABLE countries_pg (
  country_code        VARCHAR2(10) PRIMARY KEY,
  country_name        VARCHAR2(100),
  country_json        JSON
) ANNOTATIONS (
  Description 'Country 顶点表；由 normalized_production_countries.csv 装载。表达电影制作/发行国家节点，与 Movie 通过 PRODUCED_IN/RELEASED_IN 关系连接。',
  country_code '国家代码主键',
  country_name '国家名称',
  country_json '国家扩展属性 JSON'
);

-- -----------------------------------------------------------------------------
-- 4) 正式边表
-- -----------------------------------------------------------------------------
CREATE TABLE movie_cast_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_id             NUMBER NOT NULL,
  person_id           NUMBER NOT NULL,
  character_name      VARCHAR2(2000),
  cast_id             NUMBER,
  rel_json            JSON,
  CONSTRAINT fk_cast_movie FOREIGN KEY (tmdb_id) REFERENCES movies_pg(tmdb_id),
  CONSTRAINT fk_cast_person FOREIGN KEY (person_id) REFERENCES persons_pg(person_id)
) ANNOTATIONS (
  Description 'ACTED_IN 边表；数据来自 normalized_cast.csv。表示演员出演电影关系，Person(actor) -> Movie，附带角色名与 cast_id。',
  rel_id 'ACTED_IN 边主键',
  tmdb_id '目标 Movie 顶点 ID',
  person_id '源 Person(actor) 顶点 ID',
  character_name '演员在电影中饰演的角色名',
  cast_id 'cast 记录标识',
  rel_json 'ACTED_IN 边扩展属性 JSON'
);

CREATE TABLE movie_crew_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_id             NUMBER NOT NULL,
  person_id           NUMBER NOT NULL,
  job_name            VARCHAR2(100),
  department_name     VARCHAR2(100),
  rel_json            JSON,
  CONSTRAINT fk_crew_movie FOREIGN KEY (tmdb_id) REFERENCES movies_pg(tmdb_id),
  CONSTRAINT fk_crew_person FOREIGN KEY (person_id) REFERENCES persons_pg(person_id)
) ANNOTATIONS (
  Description 'Crew 关系边表；数据来自 normalized_crew.csv。job_name=Director 表示 DIRECTED，job_name=Producer 表示 PRODUCED，其余为一般 CREW 关系。',
  rel_id 'Crew 边主键',
  tmdb_id '目标 Movie 顶点 ID',
  person_id '源 Person(crew) 顶点 ID',
  job_name '岗位名称，如 Director / Producer',
  department_name '部门名称，如 Directing / Production',
  rel_json 'Crew 边扩展属性 JSON'
);

CREATE TABLE movie_genre_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_id             NUMBER NOT NULL,
  genre_id            NUMBER NOT NULL,
  rel_json            JSON,
  CONSTRAINT fk_mg_movie FOREIGN KEY (tmdb_id) REFERENCES movies_pg(tmdb_id),
  CONSTRAINT fk_mg_genre FOREIGN KEY (genre_id) REFERENCES genres_pg(genre_id)
) ANNOTATIONS (
  Description 'HAS_GENRE / IN_GENRE 边表；数据来自 normalized_genres.csv。表示电影属于某个题材，Movie -> Genre。',
  rel_id 'IN_GENRE 边主键',
  tmdb_id '源 Movie 顶点 ID',
  genre_id '目标 Genre 顶点 ID',
  rel_json '题材关系边扩展属性 JSON'
);

CREATE TABLE movie_production_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_id             NUMBER NOT NULL,
  company_id          NUMBER NOT NULL,
  rel_json            JSON,
  CONSTRAINT fk_mp_movie FOREIGN KEY (tmdb_id) REFERENCES movies_pg(tmdb_id),
  CONSTRAINT fk_mp_company FOREIGN KEY (company_id) REFERENCES production_companies_pg(company_id)
) ANNOTATIONS (
  Description 'PRODUCED_BY 边表；数据来自 normalized_production_companies.csv。表示电影由某制片公司制作，Movie -> ProductionCompany。',
  rel_id 'PRODUCED_BY 边主键',
  tmdb_id '源 Movie 顶点 ID',
  company_id '目标 ProductionCompany 顶点 ID',
  rel_json '制片公司关系边扩展属性 JSON'
);

CREATE TABLE user_rating_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id             NUMBER NOT NULL,
  tmdb_id             NUMBER NOT NULL,
  rating_value        NUMBER(3,1),
  rated_at            TIMESTAMP,
  rel_json            JSON,
  CONSTRAINT fk_ur_user FOREIGN KEY (user_id) REFERENCES users_pg(user_id),
  CONSTRAINT fk_ur_movie FOREIGN KEY (tmdb_id) REFERENCES movies_pg(tmdb_id)
) ANNOTATIONS (
  Description 'RATED 边表；数据来自 normalized_ratings_small.csv，并通过 normalized_links.csv 映射 movieId 到 tmdbId。表示用户对电影的评分关系，User -> Movie。',
  rel_id 'RATED 边主键',
  user_id '源 User 顶点 ID',
  tmdb_id '目标 Movie 顶点 ID',
  rating_value '用户对电影的评分值',
  rated_at '评分时间',
  rel_json '评分关系边扩展属性 JSON'
);

CREATE TABLE movie_language_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_id             NUMBER NOT NULL,
  language_code       VARCHAR2(10) NOT NULL,
  rel_json            JSON,
  CONSTRAINT fk_ml_movie FOREIGN KEY (tmdb_id) REFERENCES movies_pg(tmdb_id),
  CONSTRAINT fk_ml_lang FOREIGN KEY (language_code) REFERENCES spoken_languages_pg(language_code)
) ANNOTATIONS (
  Description 'HAS_LANGUAGE / SPOKEN_IN 边表；数据来自 normalized_spoken_languages.csv。表示电影包含某种语言，Movie -> SpokenLanguage。',
  rel_id 'SPOKEN_IN 边主键',
  tmdb_id '源 Movie 顶点 ID',
  language_code '目标 SpokenLanguage 顶点 ID',
  rel_json '语言关系边扩展属性 JSON'
);

CREATE TABLE movie_release_country_rel (
  rel_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_id             NUMBER NOT NULL,
  country_code        VARCHAR2(10) NOT NULL,
  rel_json            JSON,
  CONSTRAINT fk_mr_movie FOREIGN KEY (tmdb_id) REFERENCES movies_pg(tmdb_id),
  CONSTRAINT fk_mr_country FOREIGN KEY (country_code) REFERENCES countries_pg(country_code)
) ANNOTATIONS (
  Description 'PRODUCED_IN / RELEASED_IN 边表；数据来自 normalized_production_countries.csv。表示电影与某国家存在制作/发行关系，Movie -> Country。',
  rel_id 'PRODUCED_IN/RELEASED_IN 边主键',
  tmdb_id '源 Movie 顶点 ID',
  country_code '目标 Country 顶点 ID',
  rel_json '国家关系边扩展属性 JSON'
);

-- -----------------------------------------------------------------------------
-- 4.1 推荐/图查询相关索引
-- -----------------------------------------------------------------------------
-- 作用：
--   1) 优化 7.2、7.3 以及第 9 节推荐场景的连接与过滤。
--   2) 特别是 7.3 会频繁经过“高分评分 -> 电影 -> genre -> 其它电影”这条路径，
--      因此需要为评分表、题材关系表建立组合索引。
CREATE INDEX idx_rating_user_score_movie ON user_rating_rel (user_id, rating_value, tmdb_id);
CREATE INDEX idx_rating_movie_user       ON user_rating_rel (tmdb_id, user_id);
CREATE INDEX idx_genre_movie             ON movie_genre_rel (tmdb_id, genre_id);
CREATE INDEX idx_genre_genre_movie       ON movie_genre_rel (genre_id, tmdb_id);
CREATE INDEX idx_cast_movie_person       ON movie_cast_rel (tmdb_id, person_id);
CREATE INDEX idx_crew_movie_person       ON movie_crew_rel (tmdb_id, person_id);

-- -----------------------------------------------------------------------------
-- 5) staging -> 正式表装载（对应 graph_build.py）
-- -----------------------------------------------------------------------------
-- 5.0 便于反复调试：只清空数据，保留表结构
-- 作用：
--   1) 便于反复调试第 5 节装载逻辑，而不必重新 DROP/CREATE 表。
--   2) 先清空边表，再清空顶点表，避免外键约束导致 TRUNCATE 失败。
--   3) stage 表默认不清空，这样可以反复测试正式表装载；如果要重新 load CSV，再手工取消注释 stage 的 TRUNCATE。
TRUNCATE TABLE movie_cast_rel;
TRUNCATE TABLE movie_crew_rel;
TRUNCATE TABLE movie_genre_rel;
TRUNCATE TABLE movie_production_rel;
TRUNCATE TABLE movie_release_country_rel;
TRUNCATE TABLE movie_language_rel;
TRUNCATE TABLE user_rating_rel;

TRUNCATE TABLE persons_pg;
TRUNCATE TABLE users_pg;
TRUNCATE TABLE genres_pg;
TRUNCATE TABLE production_companies_pg;
TRUNCATE TABLE countries_pg;
TRUNCATE TABLE spoken_languages_pg;
TRUNCATE TABLE movies_pg;

-- 如需重导 CSV，可取消注释清空 stage
-- TRUNCATE TABLE movies_pg_stage;
-- TRUNCATE TABLE genres_pg_stage;
-- TRUNCATE TABLE production_companies_pg_stage;
-- TRUNCATE TABLE countries_pg_stage;
-- TRUNCATE TABLE spoken_languages_pg_stage;
-- TRUNCATE TABLE movie_keywords_stage;
-- TRUNCATE TABLE movie_embeddings_stage;
-- TRUNCATE TABLE movie_cast_rel_stage;
-- TRUNCATE TABLE movie_crew_rel_stage;
-- TRUNCATE TABLE movie_links_stage;
-- TRUNCATE TABLE user_rating_rel_stage;

-- 5.1 Movie 顶点（按 tmdbId 去重）
-- 作用：
--   1) 以 normalized_movies.csv 作为 Movie 顶点主来源。
--   2) 参照 Neo4j 的 MERGE (m:Movie {tmdbId}) 语义，按 tmdbId 去重，只保留一条 Movie。
--   3) 在插入时完成默认值处理：字符串字段缺失时补 'None'，数值字段缺失时补 0。
--   4) adult 字段按 Neo4j 规则映射：1 -> 'Yes'，其它 -> 'No'。
-- 说明：
--   movie_id / imdb_id 不在此处直接定稿，而是在 5.3 中通过 normalized_links.csv 再回填。
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
  embedding,
  movie_json
)
SELECT
  tmdb_id,
  NULL,
  title,
  original_title,
  adult_flag,
  budget,
  revenue,
  imdb_id,
  original_language,
  release_date,
  runtime_minutes,
  NULL,
  movie_json
FROM (
  SELECT
    TO_NUMBER(tmdbId) AS tmdb_id,
    NVL(title, 'None') AS title,
    NVL(original_title, 'None') AS original_title,
    CASE
      WHEN TO_NUMBER(NVL(NULLIF(TRIM(adult), ''), '0')) = 1 THEN 'Yes'
      ELSE 'No'
    END AS adult_flag,
    TO_NUMBER(NVL(NULLIF(budget, ''), '0')) AS budget,
    TO_NUMBER(NVL(NULLIF(revenue, ''), '0')) AS revenue,
    NVL(imdb_id, 'None') AS imdb_id,
    NVL(original_language, 'None') AS original_language,
    CASE
      WHEN release_date IS NOT NULL AND release_date <> ''
        THEN TO_DATE(release_date, 'YYYY-MM-DD')
      ELSE NULL
    END AS release_date,
    TO_NUMBER(NVL(NULLIF(runtime, ''), '0')) AS runtime_minutes,
    JSON_OBJECT(
      'tagline' VALUE NVL(tagline, 'None'),
      'overview' VALUE NVL(overview, 'None'),
      'belongs_to_collection' VALUE NVL(belongs_to_collection, 'None')
    ) AS movie_json,
    ROW_NUMBER() OVER (
      PARTITION BY TO_NUMBER(tmdbId)
      ORDER BY TO_NUMBER(tmdbId)
    ) AS rn
  FROM movies_pg_stage
  WHERE tmdbId IS NOT NULL
)
WHERE rn = 1;

-- 5.2 其它顶点
-- 作用：
--   1) 把 Genre / ProductionCompany / Country / SpokenLanguage / Person / User 等基础实体先落成顶点表。
--   2) 这些顶点会被 5.4 的各类边表直接引用，因此必须先装顶点、后装边。
--   3) Person 顶点分两批装载：
--      - cast 生成 ACTOR
--      - crew 生成 DIRECTOR / PRODUCER / CREW
--      并通过 NOT EXISTS 避免同一 person_id 重复插入。
INSERT INTO genres_pg (genre_id, genre_name, genre_json)
SELECT DISTINCT TO_NUMBER(genre_id), genre_name, JSON('{}')
FROM genres_pg_stage
WHERE genre_id IS NOT NULL;

INSERT INTO production_companies_pg (company_id, company_name, company_json)
SELECT DISTINCT TO_NUMBER(company_id), company_name, JSON('{}')
FROM production_companies_pg_stage
WHERE company_id IS NOT NULL;

INSERT INTO countries_pg (country_code, country_name, country_json)
SELECT DISTINCT country_code, country_name, JSON('{}')
FROM countries_pg_stage
WHERE country_code IS NOT NULL;

INSERT INTO spoken_languages_pg (language_code, language_name, language_json)
SELECT DISTINCT language_code, language_name, JSON('{}')
FROM spoken_languages_pg_stage
WHERE language_code IS NOT NULL;

INSERT INTO persons_pg (person_id, person_name, role_type, person_json)
SELECT person_id, person_name, role_type, person_json
FROM (
  SELECT
    TO_NUMBER(actor_id) AS person_id,
    MAX(name) AS person_name,
    'ACTOR' AS role_type,
    JSON('{}') AS person_json
  FROM movie_cast_rel_stage
  WHERE actor_id IS NOT NULL
  GROUP BY TO_NUMBER(actor_id)
)
WHERE NOT EXISTS (
  SELECT 1 FROM persons_pg p WHERE p.person_id = person_id
);

INSERT INTO persons_pg (person_id, person_name, role_type, person_json)
SELECT
  TO_NUMBER(crew_id),
  MAX(name),
  CASE
    WHEN MAX(CASE WHEN UPPER(NVL(job,'X')) = 'DIRECTOR' THEN 1 ELSE 0 END) = 1 THEN 'DIRECTOR'
    WHEN MAX(CASE WHEN UPPER(NVL(job,'X')) = 'PRODUCER' THEN 1 ELSE 0 END) = 1 THEN 'PRODUCER'
    ELSE 'CREW'
  END,
  JSON('{}')
FROM movie_crew_rel_stage c
WHERE crew_id IS NOT NULL
GROUP BY TO_NUMBER(crew_id)
HAVING NOT EXISTS (
  SELECT 1 FROM persons_pg p WHERE p.person_id = TO_NUMBER(c.crew_id)
);

INSERT INTO users_pg (user_id, user_name, user_json)
SELECT DISTINCT TO_NUMBER(userId), 'user_' || userId, JSON('{}')
FROM user_rating_rel_stage
WHERE userId IS NOT NULL;

-- 5.3 links / keywords 回填 Movie
-- 作用：
--   1) 利用 normalized_links.csv 把 movieId / imdbId 补回 movies_pg。
--      - tmdbId 是 Movie 顶点主键
--      - movieId 是评分文件 normalized_ratings_small.csv 使用的电影标识
--      因此 user_rating_rel 装载前，必须先把 movie_id 回填到 movies_pg。
--   2) 利用 normalized_keywords.csv 把 keywords 作为 Movie 的补充属性写回 movie_json。
--      keywords 更适合作为电影属性，而不是单独拆成顶点/边，因此这里采用 JSON 合并。
--   3) 利用 movie_embeddings.csv 把 embedding 回填到 movies_pg.embedding。
--      这里参考 Neo4j 的 SET m.embedding = apoc.convert.fromJsonList(row.embedding)，
--      在 Oracle 中改用原生 VECTOR 类型字段存储，便于后续向量检索/相似度计算。
--   4) movie_embeddings.csv 中的 overview 也会补充回 movie_json，字段名保持为 overview。
--      这样可以明确保留 embedding 文件中的简介字段来源，同时不改变 Movie 属性命名。
UPDATE movies_pg m
SET (movie_id, imdb_id) = (
  SELECT TO_NUMBER(l.movieId), l.imdbId
  FROM movie_links_stage l
  WHERE TO_NUMBER(l.tmdbId) = m.tmdb_id
  FETCH FIRST 1 ROW ONLY
)
WHERE EXISTS (
  SELECT 1 FROM movie_links_stage l WHERE TO_NUMBER(l.tmdbId) = m.tmdb_id
);

UPDATE movies_pg m
SET movie_json = JSON_MERGEPATCH(
  movie_json,
  JSON_OBJECT(
    'keywords' VALUE (
      SELECT k.keywords
      FROM movie_keywords_stage k
      WHERE TO_NUMBER(k.tmdbId) = m.tmdb_id
      FETCH FIRST 1 ROW ONLY
    )
  )
)
WHERE EXISTS (
  SELECT 1 FROM movie_keywords_stage k WHERE TO_NUMBER(k.tmdbId) = m.tmdb_id
);

UPDATE movies_pg m
SET embedding = (
  SELECT TO_VECTOR(e.embedding)
  FROM movie_embeddings_stage e
  WHERE TO_NUMBER(e.tmdbId) = m.tmdb_id
  FETCH FIRST 1 ROW ONLY
)
WHERE EXISTS (
  SELECT 1 FROM movie_embeddings_stage e WHERE TO_NUMBER(e.tmdbId) = m.tmdb_id
);

UPDATE movies_pg m
SET movie_json = JSON_MERGEPATCH(
  movie_json,
  JSON_OBJECT(
    'overview' VALUE (
      SELECT NVL(e.overview, 'None')
      FROM movie_embeddings_stage e
      WHERE TO_NUMBER(e.tmdbId) = m.tmdb_id
      FETCH FIRST 1 ROW ONLY
    )
  )
)
WHERE EXISTS (
  SELECT 1 FROM movie_embeddings_stage e WHERE TO_NUMBER(e.tmdbId) = m.tmdb_id
);

-- 5.4 关系边：统一 JOIN 顶点，避免外键问题
-- 作用：
--   1) 在顶点都已落表后，再统一装载关系边。
--   2) 通过 JOIN 正式顶点表的方式生成边，而不是直接从 stage 生硬插入，
--      这样可以天然保证父顶点存在，减少 ORA-02291 等外键错误。
--   3) 各边的语义如下：
--      - movie_genre_rel              : Movie -> Genre
--      - movie_production_rel         : Movie -> ProductionCompany
--      - movie_release_country_rel    : Movie -> Country
--      - movie_language_rel           : Movie -> SpokenLanguage
--      - movie_cast_rel               : Person(actor) -> Movie
--      - movie_crew_rel               : Person(crew)  -> Movie
--      - user_rating_rel              : User -> Movie
--   4) user_rating_rel 依赖 5.3 已回填好的 movies_pg.movie_id，因此必须放在 links 回填之后。
INSERT INTO movie_genre_rel (tmdb_id, genre_id, rel_json)
SELECT DISTINCT m.tmdb_id, g.genre_id, JSON('{}')
FROM genres_pg_stage s
JOIN movies_pg m ON m.tmdb_id = TO_NUMBER(s.tmdbId)
JOIN genres_pg g ON g.genre_id = TO_NUMBER(s.genre_id)
WHERE s.tmdbId IS NOT NULL
  AND s.genre_id IS NOT NULL;

INSERT INTO movie_production_rel (tmdb_id, company_id, rel_json)
SELECT DISTINCT m.tmdb_id, c.company_id, JSON('{}')
FROM production_companies_pg_stage s
JOIN movies_pg m ON m.tmdb_id = TO_NUMBER(s.tmdbId)
JOIN production_companies_pg c ON c.company_id = TO_NUMBER(s.company_id)
WHERE s.tmdbId IS NOT NULL
  AND s.company_id IS NOT NULL;

INSERT INTO movie_release_country_rel (tmdb_id, country_code, rel_json)
SELECT DISTINCT m.tmdb_id, c.country_code, JSON('{}')
FROM countries_pg_stage s
JOIN movies_pg m ON m.tmdb_id = TO_NUMBER(s.tmdbId)
JOIN countries_pg c ON c.country_code = s.country_code
WHERE s.tmdbId IS NOT NULL
  AND s.country_code IS NOT NULL;

INSERT INTO movie_language_rel (tmdb_id, language_code, rel_json)
SELECT DISTINCT m.tmdb_id, l.language_code, JSON('{}')
FROM spoken_languages_pg_stage s
JOIN movies_pg m ON m.tmdb_id = TO_NUMBER(s.tmdbId)
JOIN spoken_languages_pg l ON l.language_code = s.language_code
WHERE s.tmdbId IS NOT NULL
  AND s.language_code IS NOT NULL;

INSERT INTO movie_cast_rel (tmdb_id, person_id, character_name, cast_id, rel_json)
SELECT DISTINCT
  m.tmdb_id,
  p.person_id,
  NVL(s.character, 'None'),
  TO_NUMBER(NVL(NULLIF(s.cast_id, ''), '0')),
  JSON('{}')
FROM movie_cast_rel_stage s
JOIN movies_pg m ON m.tmdb_id = TO_NUMBER(s.tmdbId)
JOIN persons_pg p ON p.person_id = TO_NUMBER(s.actor_id)
WHERE s.tmdbId IS NOT NULL
  AND s.actor_id IS NOT NULL;

INSERT INTO movie_crew_rel (tmdb_id, person_id, job_name, department_name, rel_json)
SELECT DISTINCT
  m.tmdb_id,
  p.person_id,
  s.job,
  CASE
    WHEN UPPER(NVL(s.job,'X')) = 'DIRECTOR' THEN 'Directing'
    WHEN UPPER(NVL(s.job,'X')) = 'PRODUCER' THEN 'Production'
    ELSE 'Unknown'
  END,
  JSON('{}')
FROM movie_crew_rel_stage s
JOIN movies_pg m ON m.tmdb_id = TO_NUMBER(s.tmdbId)
JOIN persons_pg p ON p.person_id = TO_NUMBER(s.crew_id)
WHERE s.tmdbId IS NOT NULL
  AND s.crew_id IS NOT NULL;

INSERT INTO user_rating_rel (user_id, tmdb_id, rating_value, rated_at, rel_json)
SELECT DISTINCT
  u.user_id,
  m.tmdb_id,
  TO_NUMBER(r.rating),
  CASE
    WHEN r.timestamp IS NOT NULL AND r.timestamp <> ''
      THEN TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
           + NUMTODSINTERVAL(TO_NUMBER(r.timestamp), 'SECOND')
    ELSE NULL
  END,
  JSON('{}')
FROM user_rating_rel_stage r
JOIN users_pg u ON u.user_id = TO_NUMBER(r.userId)
JOIN movies_pg m ON m.movie_id = TO_NUMBER(r.movieId)
WHERE r.userId IS NOT NULL
  AND r.movieId IS NOT NULL
  AND r.rating IS NOT NULL;

COMMIT;

-- 5.5 收集统计信息
-- 作用：
--   1) 在正式顶点表与边表装载完成后，收集优化器统计信息。
--   2) 让后续 SQL / GRAPH_TABLE / 向量查询在执行计划上更稳定。
--   3) 特别适合本脚本这种“批量导入后集中查询”的场景。
--
-- 说明：
--   1) 这里使用 DBMS_STATS.GATHER_TABLE_STATS 对本 schema 下的核心表逐个收集统计信息。
--   2) ESTIMATE_PERCENT 使用 AUTO_SAMPLE_SIZE，由 Oracle 自动决定采样比例。
--   3) METHOD_OPT 使用 FOR ALL COLUMNS SIZE AUTO，让 Oracle 自动判断是否需要直方图。
BEGIN
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'MOVIES_PG',                estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'PERSONS_PG',               estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'GENRES_PG',                estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'PRODUCTION_COMPANIES_PG',  estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'USERS_PG',                 estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'SPOKEN_LANGUAGES_PG',      estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'COUNTRIES_PG',             estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);

  DBMS_STATS.GATHER_TABLE_STATS(USER, 'MOVIE_CAST_REL',           estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'MOVIE_CREW_REL',           estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'MOVIE_GENRE_REL',          estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'MOVIE_PRODUCTION_REL',     estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'MOVIE_RELEASE_COUNTRY_REL',estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'MOVIE_LANGUAGE_REL',       estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
  DBMS_STATS.GATHER_TABLE_STATS(USER, 'USER_RATING_REL',          estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, method_opt => 'FOR ALL COLUMNS SIZE AUTO', cascade => TRUE);
END;
/

-- -----------------------------------------------------------------------------
-- 6) Property Graph 定义
-- -----------------------------------------------------------------------------
CREATE PROPERTY GRAPH movie_graph_full_pg
  VERTEX TABLES (
    movies_pg AS movie
      KEY (tmdb_id)
      PROPERTIES (tmdb_id, movie_id, title, original_title, adult_flag, budget, revenue,
                  imdb_id, original_language, release_date, runtime_minutes, embedding, movie_json),
    persons_pg AS person
      KEY (person_id)
      PROPERTIES (person_id, person_name, role_type, person_json),
    genres_pg AS genre
      KEY (genre_id)
      PROPERTIES (genre_id, genre_name, genre_json),
    production_companies_pg AS production_company
      KEY (company_id)
      PROPERTIES (company_id, company_name, company_json),
    users_pg AS app_user
      KEY (user_id)
      PROPERTIES (user_id, user_name, user_json),
    spoken_languages_pg AS language
      KEY (language_code)
      PROPERTIES (language_code, language_name, language_json),
    countries_pg AS country
      KEY (country_code)
      PROPERTIES (country_code, country_name, country_json)
  )
  EDGE TABLES (
    movie_cast_rel AS acted_in
      KEY (rel_id)
      SOURCE KEY (person_id) REFERENCES person (person_id)
      DESTINATION KEY (tmdb_id) REFERENCES movie (tmdb_id)
      PROPERTIES (rel_id, character_name, cast_id, rel_json),
    movie_crew_rel AS directed
      KEY (rel_id)
      SOURCE KEY (person_id) REFERENCES person (person_id)
      DESTINATION KEY (tmdb_id) REFERENCES movie (tmdb_id)
      PROPERTIES (rel_id, job_name, department_name, rel_json),
    movie_genre_rel AS in_genre
      KEY (rel_id)
      SOURCE KEY (tmdb_id) REFERENCES movie (tmdb_id)
      DESTINATION KEY (genre_id) REFERENCES genre (genre_id)
      PROPERTIES (rel_id, rel_json),
    movie_production_rel AS produced_by
      KEY (rel_id)
      SOURCE KEY (tmdb_id) REFERENCES movie (tmdb_id)
      DESTINATION KEY (company_id) REFERENCES production_company (company_id)
      PROPERTIES (rel_id, rel_json),
    user_rating_rel AS rated
      KEY (rel_id)
      SOURCE KEY (user_id) REFERENCES app_user (user_id)
      DESTINATION KEY (tmdb_id) REFERENCES movie (tmdb_id)
      PROPERTIES (rel_id, rating_value, rated_at, rel_json),
    movie_language_rel AS spoken_in
      KEY (rel_id)
      SOURCE KEY (tmdb_id) REFERENCES movie (tmdb_id)
      DESTINATION KEY (language_code) REFERENCES language (language_code)
      PROPERTIES (rel_id, rel_json),
    movie_release_country_rel AS released_in
      KEY (rel_id)
      SOURCE KEY (tmdb_id) REFERENCES movie (tmdb_id)
      DESTINATION KEY (country_code) REFERENCES country (country_code)
      PROPERTIES (rel_id, rel_json)
  );

-- -----------------------------------------------------------------------------
-- 7) 查询示例
-- -----------------------------------------------------------------------------
-- 7.1 关系数量统计（对照 Neo4j Relationship Counts）
SELECT 'HAS_GENRE'   AS relationship_name, COUNT(*) AS relationship_count FROM movie_genre_rel
UNION ALL
SELECT 'PRODUCED_BY' AS relationship_name, COUNT(*) AS relationship_count FROM movie_production_rel
UNION ALL
SELECT 'PRODUCED_IN' AS relationship_name, COUNT(*) AS relationship_count FROM movie_release_country_rel
UNION ALL
SELECT 'HAS_LANGUAGE' AS relationship_name, COUNT(*) AS relationship_count FROM movie_language_rel
UNION ALL
SELECT 'ACTED_IN'    AS relationship_name, COUNT(*) AS relationship_count FROM movie_cast_rel
UNION ALL
SELECT 'DIRECTED'    AS relationship_name, COUNT(*) AS relationship_count FROM movie_crew_rel WHERE UPPER(job_name) = 'DIRECTOR'
UNION ALL
SELECT 'PRODUCED'    AS relationship_name, COUNT(*) AS relationship_count FROM movie_crew_rel WHERE UPPER(job_name) = 'PRODUCER'
UNION ALL
SELECT 'RATED'       AS relationship_name, COUNT(*) AS relationship_count FROM user_rating_rel;

-- 7.2 图可视化查询：用户评分电影
-- 场景：
--   用于在 SQL Developer / APEX / 图可视化工具中，把“用户给电影打分”的关系直接渲染成图。
--
-- 查询逻辑：
--   1) 在属性图 movie_graph_full_pg 中匹配：
--        (u:User)-[r:RATED]->(m:Movie)
--   2) 取出可视化通常需要的 4 个核心标识：
--        - VERTEX_ID(u)              : 当前返回行的主顶点 ID
--        - EDGE_ID(r)                : 边 ID
--        - VERTEX_ID(u)              : 源顶点 ID（用户）
--        - VERTEX_ID(m)              : 目标顶点 ID（电影）
--   3) 同时补充展示属性：
--        - user_name
--        - movie_title
--        - rating_value
--   4) 最后按评分从高到低、用户名排序，方便优先看到高分行为。
--
-- 适用场景：
--   1) 验证 User -> Movie 的评分边是否已正确装载。
--   2) 给图可视化组件提供 source/target/edge 的标准结果集。
--   3) 演示 Property Graph 中最基础、最直观的一类业务关系：用户行为图。
SELECT
  vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  user_name,
  movie_title,
  rating_value
FROM GRAPH_TABLE (
  movie_graph_full_pg
  MATCH (u IS app_user) -[r IS rated]-> (m IS movie)
  COLUMNS (
    VERTEX_ID(u) AS vertex_id,
    EDGE_ID(r) AS edge_id,
    VERTEX_ID(u) AS source_vertex_id,
    VERTEX_ID(m) AS destination_vertex_id,
    u.user_name AS user_name,
    m.title AS movie_title,
    r.rating_value AS rating_value
  )
)
ORDER BY rating_value DESC, user_name;

-- 7.3 推荐查询：高分电影 -> 相同 Genre 的其它电影（优化版）
-- 场景：
--   给“单个用户”做解释型推荐：因为你高分喜欢过某部电影，所以推荐同题材的其它电影。
--
-- 为什么要改写：
--   原始 GRAPH_TABLE 多跳扩展版本在数据量大时容易产生很大的中间结果集：
--     User -> Rated -> Movie -> Genre -> Other Movie
--   并且如果不限定用户范围，会对所有用户同时展开，执行计划通常会偏重、偏慢。
--
-- 当前优化思路：
--   1) 改成“单用户”查询，使用绑定变量 :target_user_id。
--   2) 先从 user_rating_rel 中拿到该用户的高分电影，尽量缩小起始集合。
--   3) 再通过 movie_genre_rel 做两次连接，利用 genre_id 找候选电影。
--   4) 在外层按 recommended_movie 去重，避免同一候选电影因多个共享 Genre 重复出现。
--   5) 增加 Top-N 限制，只返回最有代表性的 100 条结果。
--
-- 对应索引（已在 4.1 节创建）：
--   - idx_rating_user_score_movie (user_id, rating_value, tmdb_id)
--   - idx_genre_movie             (tmdb_id, genre_id)
--   - idx_genre_genre_movie       (genre_id, tmdb_id)
--
-- 使用方式：
--   :target_user_id 表示“当前要为哪个用户做推荐”的用户主键。
--   它对应 users_pg.user_id，也是 user_rating_rel.user_id 的外键值。
--   例如：
--     :target_user_id := 1
--   表示“基于 user_id = 1 这个用户的历史评分记录，生成推荐结果”。
--
--   为什么必须显式传这个变量：
--   1) 7.3 当前是单用户优化版，性能依赖先缩小到一个用户范围。
--   2) 不传用户 ID，就无法确定要基于谁的高分电影去推荐。
--   3) 这也是它比原始全量扩展版更快的重要原因之一。
--
--   你可以先用下面 SQL 查看有哪些用户可选：
--     SELECT user_id, user_name FROM users_pg ORDER BY user_id FETCH FIRST 20 ROWS ONLY;
--
--   先执行：
--     VAR target_user_id NUMBER;
--     EXEC :target_user_id := 1;
--   再执行下面查询。
WITH liked_movies AS (
  SELECT DISTINCT
    u.user_name,
    m.tmdb_id,
    m.title AS liked_movie
  FROM user_rating_rel r
  JOIN users_pg u  ON u.user_id = r.user_id
  JOIN movies_pg m ON m.tmdb_id = r.tmdb_id
  WHERE r.user_id = :target_user_id
    AND r.rating_value >= 4.5
),
path_candidates AS (
  SELECT DISTINCT
    lm.user_name,
    lm.liked_movie,
    g.genre_name,
    m2.tmdb_id AS recommended_tmdb_id,
    m2.title   AS recommended_movie
  FROM liked_movies lm
  JOIN movie_genre_rel mg1 ON mg1.tmdb_id = lm.tmdb_id
  JOIN genres_pg g         ON g.genre_id = mg1.genre_id
  JOIN movie_genre_rel mg2 ON mg2.genre_id = mg1.genre_id
  JOIN movies_pg m2        ON m2.tmdb_id = mg2.tmdb_id
  WHERE m2.tmdb_id <> lm.tmdb_id
    AND NOT EXISTS (
      SELECT 1
      FROM user_rating_rel r2
      WHERE r2.user_id = :target_user_id
        AND r2.tmdb_id = m2.tmdb_id
    )
)
SELECT
  user_name,
  liked_movie,
  MIN(genre_name) AS genre_name,
  recommended_movie
FROM path_candidates
GROUP BY user_name, liked_movie, recommended_tmdb_id, recommended_movie
ORDER BY user_name, liked_movie, recommended_movie
FETCH FIRST 100 ROWS ONLY;

-- 7.4 图可视化查询：电影与题材关系
-- 场景：
--   用于把 Movie -> Genre 的结构直接渲染成图，适合验证题材边是否装载正确，
--   也适合演示电影分类网络。
--
-- 查询逻辑：
--   1) 在属性图中匹配：
--        (m:Movie)-[r:IN_GENRE]->(g:Genre)
--   2) 输出图可视化需要的 vertex_id / edge_id / source / destination。
--   3) 同时补充 movie_title 与 genre_name 作为展示属性。
--   4) 为避免一次性返回过大的电影题材网络，这里限制只输出前 100 条。
--      如果你需要更多结果，可以把 100 调大，或者增加 WHERE 条件缩小范围。
SELECT
  movie_vertex_id,
  edge_id,
  source_vertex_id,
  destination_vertex_id,
  movie_title,
  genre_name
FROM GRAPH_TABLE (
  movie_graph_full_pg
  MATCH (m IS movie) -[r IS in_genre]-> (g IS genre)
  COLUMNS (
    VERTEX_ID(m) AS movie_vertex_id,
    EDGE_ID(r) AS edge_id,
    VERTEX_ID(m) AS source_vertex_id,
    VERTEX_ID(g) AS destination_vertex_id,
    m.title AS movie_title,
    g.genre_name AS genre_name
  )
)
ORDER BY genre_name, movie_title


-- 7.5 图可视化查询：出演电影数最多的演员 Top 10 及其出演关系
-- 场景：
--   用于在图工具中聚焦“最活跃的演员群体”，展示出演电影数最多的前 10 位演员，
--   以及他们与电影之间的 ACTED_IN 关系。
--
-- 查询逻辑：
--   1) 先在属性图中统计每位演员出演了多少部电影。
--   2) 取出演电影数最多的前 10 位演员。
--   3) 再把这 10 位演员的全部出演关系展开，用于图可视化。
--   4) 返回 actor_name / movie_title / character_name，并保留可视化所需的 ID 字段。
WITH top_actors AS (
  SELECT actor_name, movie_count
  FROM (
    SELECT
      actor_name,
      COUNT(DISTINCT movie_title) AS movie_count,
      ROW_NUMBER() OVER (
        ORDER BY COUNT(DISTINCT movie_title) DESC, actor_name ASC
      ) AS rn
    FROM GRAPH_TABLE (
      movie_graph_full_pg
      MATCH (p IS person) -[r IS acted_in]-> (m IS movie)
      WHERE p.role_type = 'ACTOR'
      COLUMNS (
        p.person_name AS actor_name,
        m.title AS movie_title
      )
    )
    GROUP BY actor_name
  )
  WHERE rn <= 10
),
top_actor_edges AS (
  SELECT *
  FROM GRAPH_TABLE (
    movie_graph_full_pg
    MATCH (p IS person) -[r IS acted_in]-> (m IS movie)
    WHERE p.role_type = 'ACTOR'
    COLUMNS (
      VERTEX_ID(p) AS actor_vertex_id,
      EDGE_ID(r) AS edge_id,
      VERTEX_ID(p) AS source_vertex_id,
      VERTEX_ID(m) AS destination_vertex_id,
      p.person_name AS actor_name,
      m.title AS movie_title,
      r.character_name AS character_name
    )
  )
)
SELECT
  tae.actor_vertex_id,
  tae.edge_id,
  tae.source_vertex_id,
  tae.destination_vertex_id,
  tae.actor_name,
  tae.movie_title,
  tae.character_name,
  ta.movie_count
FROM top_actor_edges tae
JOIN top_actors ta
  ON ta.actor_name = tae.actor_name
ORDER BY ta.movie_count DESC, tae.actor_name, tae.movie_title;

-- 7.3b 推荐查询：改写为 Property Graph 语法（保留原有单用户逻辑）
-- 场景：
--   与 7.3 保持相同业务逻辑，但显式使用 GRAPH_TABLE 做属性图模式匹配。
--
-- 保留的原有逻辑：
--   1) 仍然是“单用户”推荐，使用 :target_user_id。
--   2) 仍然只取该用户 rating_value >= 4.5 的电影作为 liked_movies。
--   3) 仍然沿着图路径：
--        (u)-[rated]->(m1)-[in_genre]->(g)<-[in_genre]-(m2)
--      找到共享题材的候选电影。
--   4) 仍然排除用户已经评分过的电影。
--   5) 仍然对 recommended_movie 去重，并保留一个代表性的 genre_name。
--   6) 仍然做 Top-N 限制。
--
-- 与 7.3 的差异：
--   - 7.3 采用关系表 JOIN 写法，更利于当前执行计划优化。
--   - 7.3b 采用 Property Graph 写法，更能体现图模式语义。
--   - 如果在你的环境里 7.3 性能明显更好，建议把 7.3 作为执行版，7.3b 作为图语法对照版。
--
-- 其中 :target_user_id 的含义与 7.3 完全相同：
--   表示“当前要为哪个用户生成推荐”。
--   它对应 users_pg.user_id。
WITH graph_candidates AS (
  SELECT *
  FROM GRAPH_TABLE (
    movie_graph_full_pg
    MATCH (u IS app_user) -[r IS rated]-> (m1 IS movie) -[g1 IS in_genre]-> (g IS genre),
          (m2 IS movie) -[g2 IS in_genre]-> (g)
    WHERE u.user_id = :target_user_id
      AND r.rating_value >= 4.5
      AND m1.tmdb_id <> m2.tmdb_id
    COLUMNS (
      u.user_name AS user_name,
      m1.tmdb_id AS liked_tmdb_id,
      m1.title AS liked_movie,
      g.genre_name AS genre_name,
      m2.tmdb_id AS recommended_tmdb_id,
      m2.title AS recommended_movie
    )
  )
)
SELECT
  gc.user_name,
  gc.liked_movie,
  MIN(gc.genre_name) AS genre_name,
  gc.recommended_movie
FROM graph_candidates gc
WHERE NOT EXISTS (
  SELECT 1
  FROM user_rating_rel r2
  WHERE r2.user_id = :target_user_id
    AND r2.tmdb_id = gc.recommended_tmdb_id
)
GROUP BY gc.user_name, gc.liked_movie, gc.recommended_tmdb_id, gc.recommended_movie
ORDER BY gc.user_name, gc.liked_movie, gc.recommended_movie
FETCH FIRST 100 ROWS ONLY;

-- -----------------------------------------------------------------------------
-- 8) 利用 embedding 模型重新向量化 movies_pg.embedding
-- -----------------------------------------------------------------------------
-- 作用：
--   1) 不依赖 movie_embeddings.csv，直接用 Oracle 的 embedding 模型重新生成 Movie 向量。
--   2) 适用于你想切换模型、重新生成向量、或校正已有 embedding 的场景。
--   3) 推荐把 title + overview 组合作为输入文本，这样比只用标题信息更完整。
--
-- 前提：
--   1) 数据库中已经可用对应的 embedding model。
--   2) 当前 schema 具备调用向量化模型的权限。
--   3) 下方模型名 'YOUR_EMBEDDING_MODEL' 需要替换成你实际可用的模型名。
--
-- 输入文本策略：
--   title || ' : ' || overview
--   其中 overview 优先取 movie_json.overview；如果没有，则回退为 'None'。
--
-- 示例：重新为全部 Movie 生成 embedding
-- UPDATE movies_pg m
-- SET embedding = VECTOR_EMBEDDING(
--   YOUR_EMBEDDING_MODEL
--   USING (
--     m.title || ' : ' ||
--     COALESCE(
--       JSON_VALUE(m.movie_json, '$.overview' RETURNING VARCHAR2(4000)),
--       'None'
--     )
--   ) AS data
-- );
--
-- COMMIT;
--
-- 示例：只为 embedding 为空的 Movie 重新向量化
-- UPDATE movies_pg m
-- SET embedding = VECTOR_EMBEDDING(
--   YOUR_EMBEDDING_MODEL
--   USING (
--     m.title || ' : ' ||
--     COALESCE(
--       JSON_VALUE(m.movie_json, '$.overview' RETURNING VARCHAR2(4000)),
--       'None'
--     )
--   ) AS data
-- )
-- WHERE m.embedding IS NULL;
--
-- COMMIT;
--
-- 示例：只为最近一批电影重算向量（按 tmdb_id 条件筛选）
-- UPDATE movies_pg m
-- SET embedding = VECTOR_EMBEDDING(
--   YOUR_EMBEDDING_MODEL
--   USING (
--     m.title || ' : ' ||
--     COALESCE(
--       JSON_VALUE(m.movie_json, '$.overview' RETURNING VARCHAR2(4000)),
--       'None'
--     )
--   ) AS data
-- )
-- WHERE m.tmdb_id BETWEEN 1 AND 12000;
--
-- COMMIT;
--
-- 验证：检查哪些 Movie 已生成向量
-- SELECT tmdb_id, title
-- FROM movies_pg
-- WHERE embedding IS NOT NULL
-- FETCH FIRST 20 ROWS ONLY;
--
-- 验证：统计仍未生成向量的 Movie 数量
-- SELECT COUNT(*) AS movies_without_embedding
-- FROM movies_pg
-- WHERE embedding IS NULL;

-- -----------------------------------------------------------------------------
-- 9) Graph + 向量查询融合：电影推荐 Demo
-- -----------------------------------------------------------------------------
-- 场景目标：
--   结合“用户历史高分图关系”与“电影语义向量相似度”，做一个混合推荐示例。
--
-- 思路：
--   1) 先从图里找出某个用户喜欢过的电影（例如评分 >= 4.5）。
--   2) 取这些已喜欢电影的 embedding，去向量空间中找相似电影。
--   3) 再结合图里的 Genre 关系做约束或加分，得到更可解释的推荐结果。
--
-- 说明：
--   1) 下例使用 COSINE 距离作为向量相似度度量，距离越小表示越相似。
--   2) 你可以把 :target_user_id 改成具体用户 ID，例如 1。
--   3) 该示例排除了用户已经评分过的电影，避免推荐已看内容。

-- 9.1 纯“用户喜欢电影 -> 相似向量电影”推荐
-- VAR target_user_id NUMBER;
-- EXEC :target_user_id := 1;
--
-- WITH liked_movies AS (
--   SELECT DISTINCT m.tmdb_id, m.title, m.embedding
--   FROM user_rating_rel r
--   JOIN movies_pg m ON m.tmdb_id = r.tmdb_id
--   WHERE r.user_id = :target_user_id
--     AND r.rating_value >= 4.5
--     AND m.embedding IS NOT NULL
-- ),
-- candidate_movies AS (
--   SELECT
--     lm.tmdb_id AS liked_tmdb_id,
--     lm.title   AS liked_title,
--     c.tmdb_id  AS candidate_tmdb_id,
--     c.title    AS candidate_title,
--     VECTOR_DISTANCE(c.embedding, lm.embedding, COSINE) AS cosine_distance
--   FROM liked_movies lm
--   JOIN movies_pg c
--     ON c.embedding IS NOT NULL
--    AND c.tmdb_id <> lm.tmdb_id
--   WHERE NOT EXISTS (
--     SELECT 1
--     FROM user_rating_rel ur
--     WHERE ur.user_id = :target_user_id
--       AND ur.tmdb_id = c.tmdb_id
--   )
-- )
-- SELECT *
-- FROM candidate_movies
-- ORDER BY cosine_distance ASC
-- FETCH FIRST 20 ROWS ONLY;

-- 9.2 Graph + 向量融合推荐：要求候选电影与已喜欢电影至少共享一个 Genre
-- 这里“Graph 查询”的体现就在于：
--   先通过 GRAPH_TABLE 在属性图 movie_graph_full_pg 上做模式匹配，找出：
--     用户高分电影 -> Genre <- 候选电影
--   也就是：
--     (u)-[rated]->(m1)-[in_genre]->(g)<-[in_genre]-(m2)
--   然后再对 m1 / m2 的 embedding 做向量相似度计算。
--
-- VAR target_user_id NUMBER;
-- EXEC :target_user_id := 1;
--
-- WITH graph_candidates AS (
--   SELECT *
--   FROM GRAPH_TABLE (
--     movie_graph_full_pg
--     MATCH (u IS app_user) -[r IS rated]-> (m1 IS movie) -[g1 IS in_genre]-> (g IS genre),
--           (m2 IS movie) -[g2 IS in_genre]-> (g)
--     WHERE u.user_id = :target_user_id
--       AND r.rating_value >= 4.5
--       AND m1.tmdb_id <> m2.tmdb_id
--     COLUMNS (
--       m1.tmdb_id AS liked_tmdb_id,
--       m1.title AS liked_title,
--       m2.tmdb_id AS candidate_tmdb_id,
--       m2.title AS candidate_title,
--       g.genre_name AS shared_genre
--     )
--   )
-- ),
-- fused_scores AS (
--   SELECT
--     gc.liked_tmdb_id,
--     gc.liked_title,
--     gc.candidate_tmdb_id,
--     gc.candidate_title,
--     gc.shared_genre,
--     VECTOR_DISTANCE(m2.embedding, m1.embedding, COSINE) AS cosine_distance
--   FROM graph_candidates gc
--   JOIN movies_pg m1 ON m1.tmdb_id = gc.liked_tmdb_id
--   JOIN movies_pg m2 ON m2.tmdb_id = gc.candidate_tmdb_id
--   WHERE m1.embedding IS NOT NULL
--     AND m2.embedding IS NOT NULL
--     AND NOT EXISTS (
--       SELECT 1
--       FROM user_rating_rel ur
--       WHERE ur.user_id = :target_user_id
--         AND ur.tmdb_id = gc.candidate_tmdb_id
--     )
-- )
-- SELECT
--   candidate_tmdb_id,
--   candidate_title,
--   MIN(cosine_distance) AS best_cosine_distance,
--   COUNT(*) AS supporting_paths,
--   MIN(shared_genre) AS example_shared_genre
-- FROM fused_scores
-- GROUP BY candidate_tmdb_id, candidate_title
-- ORDER BY best_cosine_distance ASC, supporting_paths DESC
-- FETCH FIRST 20 ROWS ONLY;
--
-- 下面这个版本是等价的“关系表 JOIN 写法”，逻辑上与上面的 GRAPH_TABLE 一致，
-- 只是没有直接在属性图对象上做模式匹配。
-- VAR target_user_id NUMBER;
-- EXEC :target_user_id := 1;
--
-- WITH liked_movies AS (
--   SELECT DISTINCT m.tmdb_id, m.title, m.embedding
--   FROM user_rating_rel r
--   JOIN movies_pg m ON m.tmdb_id = r.tmdb_id
--   WHERE r.user_id = :target_user_id
--     AND r.rating_value >= 4.5
--     AND m.embedding IS NOT NULL
-- ),
-- graph_candidates AS (
--   SELECT DISTINCT
--     lm.tmdb_id AS liked_tmdb_id,
--     c.tmdb_id  AS candidate_tmdb_id
--   FROM liked_movies lm
--   JOIN movie_genre_rel mg1 ON mg1.tmdb_id = lm.tmdb_id
--   JOIN movie_genre_rel mg2 ON mg2.genre_id = mg1.genre_id
--   JOIN movies_pg c         ON c.tmdb_id = mg2.tmdb_id
--   WHERE c.tmdb_id <> lm.tmdb_id
--     AND c.embedding IS NOT NULL
--     AND NOT EXISTS (
--       SELECT 1
--       FROM user_rating_rel ur
--       WHERE ur.user_id = :target_user_id
--         AND ur.tmdb_id = c.tmdb_id
--     )
-- ),
-- fused_scores AS (
--   SELECT
--     lm.title AS liked_title,
--     c.title  AS candidate_title,
--     gc.candidate_tmdb_id,
--     VECTOR_DISTANCE(c.embedding, lm.embedding, COSINE) AS cosine_distance
--   FROM graph_candidates gc
--   JOIN liked_movies lm ON lm.tmdb_id = gc.liked_tmdb_id
--   JOIN movies_pg c     ON c.tmdb_id = gc.candidate_tmdb_id
-- )
-- SELECT
--   candidate_tmdb_id,
--   candidate_title,
--   MIN(cosine_distance) AS best_cosine_distance,
--   COUNT(*) AS supporting_paths
-- FROM fused_scores
-- GROUP BY candidate_tmdb_id, candidate_title
-- ORDER BY best_cosine_distance ASC, supporting_paths DESC
-- FETCH FIRST 20 ROWS ONLY;

-- 9.3 用自然语言兴趣直接生成 query embedding，再结合图约束做推荐
-- 前提：需要可用的 embedding model，把 YOUR_EMBEDDING_MODEL 替换成真实模型名。
--
-- 这里同样可以显式结合 graph 查询，而不是只做关系表过滤。
-- 例如：先在属性图里筛出属于指定 Genre 的 Movie 子图，再对这些候选电影做向量相似度排序。
--
-- WITH query_vec AS (
--   SELECT VECTOR_EMBEDDING(
--            YOUR_EMBEDDING_MODEL
--            USING 'animated family adventure toys friendship' AS data
--          ) AS qv
--   FROM dual
-- ),
-- graph_filtered_movies AS (
--   SELECT *
--   FROM GRAPH_TABLE (
--     movie_graph_full_pg
--     MATCH (m IS movie) -[mg IS in_genre]-> (g IS genre)
--     WHERE g.genre_name IN ('Animation', 'Adventure', 'Family')
--     COLUMNS (
--       m.tmdb_id AS tmdb_id,
--       m.title AS title
--     )
--   )
-- )
-- SELECT
--   gm.tmdb_id,
--   gm.title,
--   MIN(g.genre_name) AS matched_genre,
--   VECTOR_DISTANCE(gm.embedding, q.qv, COSINE) AS cosine_distance
-- FROM graph_filtered_movies gf
-- JOIN movies_pg gm ON gm.tmdb_id = gf.tmdb_id
-- JOIN movie_genre_rel mgr ON mgr.tmdb_id = gm.tmdb_id
-- JOIN genres_pg g ON g.genre_id = mgr.genre_id
-- CROSS JOIN query_vec q
-- WHERE gm.embedding IS NOT NULL
--   AND g.genre_name IN ('Animation', 'Adventure', 'Family')
-- GROUP BY gm.tmdb_id, gm.title, VECTOR_DISTANCE(gm.embedding, q.qv, COSINE)
-- ORDER BY cosine_distance ASC
-- FETCH FIRST 20 ROWS ONLY;

-- 如果希望进一步体现“自然语言 + 用户上下文 + 图约束 + 向量”的融合，
-- 还可以先从图里拿到用户喜欢的 Genre，再用自然语言 query embedding 在这些 Genre 子图内排序：
--
-- VAR target_user_id NUMBER;
-- EXEC :target_user_id := 1;
--
-- WITH query_vec AS (
--   SELECT VECTOR_EMBEDDING(
--            YOUR_EMBEDDING_MODEL
--            USING 'light-hearted animated adventure with friendship and toys' AS data
--          ) AS qv
--   FROM dual
-- ),
-- user_preferred_genres AS (
--   SELECT *
--   FROM GRAPH_TABLE (
--     movie_graph_full_pg
--     MATCH (u IS app_user) -[r IS rated]-> (m IS movie) -[mg IS in_genre]-> (g IS genre)
--     WHERE u.user_id = :target_user_id
--       AND r.rating_value >= 4.0
--     COLUMNS (
--       g.genre_id AS genre_id,
--       g.genre_name AS genre_name
--     )
--   )
-- ),
-- candidate_movies AS (
--   SELECT *
--   FROM GRAPH_TABLE (
--     movie_graph_full_pg
--     MATCH (m IS movie) -[mg IS in_genre]-> (g IS genre)
--     COLUMNS (
--       m.tmdb_id AS tmdb_id,
--       m.title AS title,
--       g.genre_id AS genre_id,
--       g.genre_name AS genre_name
--     )
--   )
-- )
-- SELECT
--   cm.tmdb_id,
--   cm.title,
--   MIN(cm.genre_name) AS matched_genre,
--   VECTOR_DISTANCE(m.embedding, q.qv, COSINE) AS cosine_distance
-- FROM candidate_movies cm
-- JOIN user_preferred_genres upg ON upg.genre_id = cm.genre_id
-- JOIN movies_pg m ON m.tmdb_id = cm.tmdb_id
-- CROSS JOIN query_vec q
-- WHERE m.embedding IS NOT NULL
--   AND NOT EXISTS (
--     SELECT 1
--     FROM user_rating_rel ur
--     WHERE ur.user_id = :target_user_id
--       AND ur.tmdb_id = cm.tmdb_id
--   )
-- GROUP BY cm.tmdb_id, cm.title, VECTOR_DISTANCE(m.embedding, q.qv, COSINE)
-- ORDER BY cosine_distance ASC
-- FETCH FIRST 20 ROWS ONLY;

-- 9.4 可解释性增强建议
--   你可以在 9.2 / 9.3 的基础上额外返回：
--   1) 共享的 Genre 名称
--   2) 候选电影被多少条图路径支持（supporting_paths）
--   3) 用户为什么会被推荐这部电影（如“你喜欢 Toy Story，而它与推荐电影在向量空间接近，且同属 Animation/Family”）

-- 说明：
-- 本脚本严格参考 graph_build.py：需要多份 normalized_*.csv，而不是单个 normalized_movies.csv。
-- 如果你手头只有 normalized_movies.csv，请使用 oracle-movie-graph-demo.sql。
