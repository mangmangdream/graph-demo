# graph-demo

Oracle Property Graph SQL demo collection.

## Files

- `graph-demo.sql` - basic graph demo
- `graph-demo-multi-rel.sql` - multi-relationship graph demo
- `graph-demo-json.sql` - graph demo with JSON data
- `knowledge-graph-json-demo.sql` - knowledge graph JSON demo
- `oracle-movie-graph-demo.sql` - simplified movie graph demo
- `oracle-movie-graph-full.sql` - full movie graph demo with property graph, graph queries, recommendation queries, and visualization examples

## Highlights

- Property Graph object creation
- Vertex / edge loading from normalized data
- Graph visualization query examples
- Recommendation query examples
- Property Graph syntax and relational SQL comparison

## Execution Order

Choose one script family and run it end to end in the same schema.

### Quick start

1. Run `graph-demo.sql` for the smallest relational + graph example.
2. Run `graph-demo-json.sql` for graph + JSON property examples.
3. Run `graph-demo-multi-rel.sql` for multi-edge modeling examples.
4. Run `knowledge-graph-json-demo.sql` for document / chunk / concept RAG-style modeling.

These four scripts are self-contained demos. They do not depend on CSV imports.

### Movie demo

1. Prepare `normalized_movies.csv`.
2. Run `oracle-movie-graph-demo.sql`.
3. Import CSV data into `MOVIES_PG_STAGE` using SQL Developer, SQLcl, SQL*Loader, or `DBMS_CLOUD`.
4. Continue running the load, graph creation, and query sections in the same script.

Use this script when you only have the single movie CSV.

### Full movie graph demo

1. Prepare all normalized CSV files listed below in `Data Preparation`.
2. Run `oracle-movie-graph-full.sql` to create staging tables, base tables, and indexes.
3. Load the CSV files into the staging tables in the order shown inside the script.
4. Run the load section that populates vertex tables and edge tables.
5. Run the `CREATE PROPERTY GRAPH movie_graph_full_pg` section.
6. Run the sample graph queries, recommendation queries, and optional vector examples.

Use this script when you want the most complete example set, including ratings, cast, crew, and embeddings.

## Dependency Versions

This repository targets Oracle Database environments that support SQL Property Graph.

- Oracle Database: Oracle 23ai or later is the practical baseline for the JSON + Property Graph examples.
- Oracle Database for full movie demo: Oracle 26ai-style environment is strongly recommended because `oracle-movie-graph-full.sql` uses `VECTOR(*, FLOAT32)`, `TO_VECTOR(...)`, and vector query examples.
- SQL client: SQL Developer or SQLcl with support for the `CREATE PROPERTY GRAPH` syntax.
- Optional import utility: SQL*Loader or `DBMS_CLOUD` if CSVs are loaded outside SQL Developer / SQLcl.

If your target environment does not support vector types or vector functions, avoid the vector parts of `oracle-movie-graph-full.sql`.

## Minimum Privileges

Run the scripts in a dedicated schema that can create and modify its own objects.

- `CREATE SESSION`
- `CREATE TABLE`
- `CREATE VIEW` if your environment or tooling requires helper views
- privilege to create Property Graph objects in the target schema
- privilege to create indexes
- privilege to insert, update, delete, and truncate tables in the target schema
- privilege to execute `DBMS_STATS` for the full movie script

Optional privileges:

- privilege to execute `DBMS_CLOUD` if you load CSV files from object storage or HTTPS
- privilege to use embedding / vector model functions if you run the optional `VECTOR_EMBEDDING(...)` examples

If you are not sure whether Property Graph creation is enabled in your database, verify it before running the larger scripts.

## Data Preparation

### Self-contained demos

These scripts include inline sample data and do not require external files:

- `graph-demo.sql`
- `graph-demo-json.sql`
- `graph-demo-multi-rel.sql`
- `knowledge-graph-json-demo.sql`

### Simplified movie demo

Prepare this file before running `oracle-movie-graph-demo.sql`:

- `normalized_movies.csv`

Load it into staging table `MOVIES_PG_STAGE`.

### Full movie graph demo

Prepare these files before running `oracle-movie-graph-full.sql`:

- `normalized_movies.csv`
- `normalized_genres.csv`
- `normalized_production_companies.csv`
- `normalized_production_countries.csv`
- `normalized_spoken_languages.csv`
- `normalized_keywords.csv`
- `movie_embeddings.csv`
- `normalized_cast.csv`
- `normalized_crew.csv`
- `normalized_links.csv`
- `normalized_ratings_small.csv`

Recommended preparation rules:

- keep CSV headers aligned with the staging table column names used in the scripts
- use UTF-8 encoding
- keep empty numeric fields blank rather than mixing placeholders such as `N/A`
- place all CSV files under a portable working directory such as `/path/to/csv-files`
- validate row counts after each import before running the load section

## Suggested Workflow

1. Start with `graph-demo.sql` to verify Property Graph syntax works in your environment.
2. Move to `graph-demo-json.sql` or `knowledge-graph-json-demo.sql` if you want JSON-heavy patterns.
3. Use `oracle-movie-graph-demo.sql` for a smaller CSV-based movie pipeline.
4. Use `oracle-movie-graph-full.sql` only after the simpler scripts run successfully.

## Notes

- Scripts are written for Oracle SQL / Property Graph scenarios.
- `oracle-movie-graph-full.sql` contains the most complete example set.
- Some scripts are repeatable and include cleanup blocks; `graph-demo.sql` is currently closer to a one-shot demo and may need manual cleanup before rerun.
