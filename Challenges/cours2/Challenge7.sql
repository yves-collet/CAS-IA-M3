-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 7. Partager la couche Gold
-- MAGIC Configurez la couche gold en lecture
-- MAGIC seule pour un de vos collègues. 

-- COMMAND ----------

USE CATALOG yvescollet_lakehouse;

-- COMMAND ----------

-- DBTITLE 1,Cell 3
GRANT USAGE ON CATALOG yvescollet_lakehouse TO `alexandre.mueller@he-arc.ch`;
GRANT USAGE ON SCHEMA yvescollet_lakehouse.gold TO `alexandre.mueller@he-arc.ch`;

GRANT SELECT ON SCHEMA yvescollet_lakehouse.gold TO `alexandre.mueller@he-arc.ch`;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA yvescollet_lakehouse.gold;
