-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 5. Finaliser la couche Gold
-- MAGIC Appropriez-vous le code de la création
-- MAGIC des tables de la couches gold ainsi que
-- MAGIC les notebooks de chargement.
-- MAGIC Identifiez des pistes d'améliorations.

-- COMMAND ----------

USE CATALOG yvescollet_lakehouse;
USE DATABASE gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Création des Dimensions (Customer, Address)

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_customer AS
SELECT
    c.customer_id,
    c.title,
    c.first_name,
    c.middle_name,
    c.last_name,
    c.suffix,
    c.company_name,
    c.sales_person,
    c.email_address,
    c.phone
FROM silver.customer c
WHERE c._tf_valid_to IS NULL;

CREATE OR REPLACE TABLE dim_address AS
SELECT
    a.address_id,
    a.address_line1,
    a.address_line2,
    a.city,
    a.state_province,
    a.country_region,
    a.postal_code
FROM silver.address a
WHERE a._tf_valid_to IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Création de la Fact Table (Sales)
-- MAGIC
-- MAGIC Note: Ajout des deux adresses (ShipTo et BillTo) pour complétude.

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_sales AS
SELECT
    h.sales_order_id,
    d.sales_order_detail_id,
    h.sales_order_number,
    h.customer_id,
    h.ship_to_address_id,
    h.bill_to_address_id,
    d.product_id,
    h.order_date,
    h.due_date,
    h.ship_date,
    h.status,
    d.order_qty,
    d.unit_price,
    d.unit_price_discount,
    d.line_total,
    h.sub_total,
    h.tax_amt,
    h.freight,
    h.total_due
FROM silver.sales_order_header h
JOIN silver.sales_order_detail d ON h.sales_order_id = d.sales_order_id
WHERE h._tf_valid_to IS NULL 
  AND d._tf_valid_to IS NULL;

-- COMMAND ----------

SELECT count(*) FROM fact_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🚀 Pistes d'améliorations proposées (Challenge 5)
-- MAGIC
-- MAGIC Voici les améliorations identifiées pour robustifier la couche Gold :
-- MAGIC
-- MAGIC ### 1. Modélisation en Étoile (Star Schema)
-- MAGIC *   **Dimension Date (`dim_date`)** : Il manque une table de temps standard pour faciliter les analyses par année, trimestre, mois, jour de semaine, vacances, etc.
-- MAGIC *   **Dimension Produit (`dim_product`)** : La table `fact_sales` référence `product_id`, mais aucune dimension produit n'est créée ici. Elle est essentielle pour analyser les ventes par catégorie, modèle, etc.
-- MAGIC *   **Clés de Substitution (Surrogate Keys)** : Utiliser des clés générées (ex: `dim_customer_sk`) plutôt que les clés métier (`customer_id`) pour gérer l'indépendance vis-à-vis de la source et l'historisation complexe.
-- MAGIC
-- MAGIC ### 2. Performance & Optimisation
-- MAGIC *   **Partitionnement** : La table de faits `fact_sales` devrait être partitionnée (ex: par année/mois de `order_date`) pour accélérer les requêtes sur des périodes spécifiques.
-- MAGIC *   **OPTIMIZE & Z-ORDER** : Appliquer un `OPTIMIZE fact_sales ORDER BY (customer_id, product_id)` permettrait de co-localiser les données fréquemment jointes ou filtrées.
-- MAGIC
-- MAGIC ### 3. Gestion de l'Historique (SCD)
-- MAGIC *   Actuellement, la requête filtre sur `_tf_valid_to IS NULL`, ne prenant que la version *actuelle* du client/adresse.
-- MAGIC *   **Amélioration** : Pour une analyse précise, on voudrait souvent savoir où habitait le client *au moment de la commande*. Il faudrait lier la Fact Table aux Dimensions via les clés valides à la date de commande (AS OF).
-- MAGIC
-- MAGIC ### 4. Stratégie de Chargement
-- MAGIC *   **Incremental** : `CREATE OR REPLACE` recharge tout à chaque fois. Pour de gros volumes, passer à une logique `MERGE` ou `INSERT OVERWRITE` sur les partitions récentes est plus efficace.
-- MAGIC
-- MAGIC ### 5. Gouvernance
-- MAGIC *   Ajouter des **commentaires** sur les tables et colonnes (`COMMENT ON TABLE ...`).
-- MAGIC *   Définir des **Contraintes** (Primary Key, Foreign Key) explicitement (même si *Informational* sur Databricks) pour aider les outils de BI et la documentation.
