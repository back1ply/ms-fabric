-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "bc5c5ff7-dbfe-46f0-af4c-e5b298762e86",
-- META       "default_lakehouse_name": "LH",
-- META       "default_lakehouse_workspace_id": "8d3001ae-2f0d-4d23-a9ee-f2698798f695",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "bc5c5ff7-dbfe-46f0-af4c-e5b298762e86"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

%sql
CREATE TABLE IF NOT EXISTS bronze._init (id INT);
DROP TABLE IF EXISTS bronze._init;

CREATE TABLE IF NOT EXISTS silver._init (id INT);
DROP TABLE IF EXISTS silver._init;

CREATE TABLE IF NOT EXISTS gold._init (id INT);
DROP TABLE IF EXISTS gold._init;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
