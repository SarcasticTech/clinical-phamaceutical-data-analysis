-- Databricks notebook source
-- Defines the Clinical Year Version

SET year_int = 2021;

-- COMMAND ----------

-- Creates the Table for 'clinicaltrial_<year>' & 'pharma'

DROP TABLE IF EXISTS clinicaltrial;
CREATE TABLE clinicaltrial
USING CSV
OPTIONS (Path 'dbfs:/FileStore/tables/clinicaltrial_${hiveconf:year_int}.csv', Header 'True', InfersSchema 'True', Delimiter '|');

DROP TABLE IF EXISTS pharma;
CREATE TABLE pharma
USING CSV
OPTIONS (Path 'dbfs:/FileStore/tables/pharma.csv', Header 'True', InfersSchema 'True');

-- COMMAND ----------

-- Checks for the 'clinicaltrial_<year>' & 'pharma' Tables

SHOW TABLES LIKE 'clinicaltrial|pharma';

-- COMMAND ----------

-- Views the Content of 'clinicaltrial_<year>'

SELECT *
FROM clinicaltrial

-- COMMAND ----------

-- Views the Content of 'pharma'

SELECT *
FROM pharma

-- COMMAND ----------

-- Question 1 - Counts the Number of Distinct Studies

SELECT DISTINCT COUNT(*) AS Total_Studies 
FROM clinicaltrial

-- COMMAND ----------

-- Question 2 - Lists the Types of Studies with Frequency

SELECT Type, COUNT(*) AS Frequency
FROM clinicaltrial
GROUP BY Type
ORDER BY Frequency DESC

-- COMMAND ----------

-- Question 3 - Lists the Top 5 Conditions with Frequency

SELECT Conditions, COUNT(*) AS Frequency
FROM (
  SELECT explode(split(Conditions, ',')) AS Conditions
  FROM clinicaltrial
  WHERE Conditions IS NOT NULL
)
GROUP BY Conditions
ORDER BY Frequency DESC
LIMIT 5

-- COMMAND ----------

-- Question 4 - Lists the Top 10 Most Common Non-Pharmaceutical Sponsors with Clinical Trials

SELECT Sponsor, COUNT(*) as Trials
FROM clinicaltrial
WHERE Sponsor NOT IN (SELECT Parent_Company FROM pharma)
GROUP BY Sponsor
ORDER BY Trials DESC
LIMIT 10

-- COMMAND ----------

-- Question 5 - Lists the Completed Studies Each Month in a Given Year

SELECT DATE_FORMAT(TO_DATE(Completion, 'MMM yyyy'), 'MMM') AS Month, COUNT(*) AS Completed_Studies 
FROM clinicaltrial
WHERE Status = 'Completed' AND YEAR(TO_DATE(Completion, 'MMM yyyy')) = ${hiveconf:year_int}
GROUP BY Month
ORDER BY TO_DATE(Month, 'MMM')

-- COMMAND ----------

-- Further Analysis 3 - Lists the Count & Percentage of Clinical Trials from Each Status

SELECT Status, COUNT(*) AS Frequency, 
    CONCAT(ROUND((COUNT(*) / SUM(COUNT(*)) OVER()) * 100, 2), '%') AS Percentage
FROM clinicaltrial
GROUP BY Status
ORDER BY Frequency DESC

-- COMMAND ----------

-- Part 3 of Creating the Visuals on Power Bi

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Transforms the Further Analysis 3 Output from SQL to DF
-- MAGIC 
-- MAGIC query = """
-- MAGIC SELECT Status, COUNT(*) AS Frequency, 
-- MAGIC     CONCAT(ROUND((COUNT(*) / SUM(COUNT(*)) OVER()) * 100, 2), '%') AS Percentage
-- MAGIC FROM clinicaltrial
-- MAGIC GROUP BY Status
-- MAGIC ORDER BY Frequency DESC
-- MAGIC """
-- MAGIC 
-- MAGIC status_ratio = spark.sql(query)
-- MAGIC 
-- MAGIC status_ratio.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Creates the Further Analysis 3 Output CSV
-- MAGIC 
-- MAGIC status_ratio.write.csv("/FileStore/tables/FA3_Output.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Creates a User-Defined Function to Check for the Question & Further Analysis CSVs
-- MAGIC 
-- MAGIC def check_file(files, path):
-- MAGIC     file_present = [dbutils.fs.ls(f"{path}/{file}") for file in files]
-- MAGIC     for file, is_present in zip(files, file_present):
-- MAGIC         if len(is_present) > 0:
-- MAGIC             print(f"{file} is present in {path}/")
-- MAGIC         else:
-- MAGIC             print(f"{file} is not present in {path}/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Checks for the Question & Further Analysis CSVs
-- MAGIC 
-- MAGIC path = "/FileStore/tables"
-- MAGIC files = ["Q2_Output.csv", 
-- MAGIC          "Q3_Output.csv", 
-- MAGIC          "Q4_Output.csv", 
-- MAGIC          "Q5_Output.csv", 
-- MAGIC          "FA1_Output.csv", 
-- MAGIC          "FA2_Output.csv", 
-- MAGIC          "FA3_Output.csv"]
-- MAGIC 
-- MAGIC check_file(files, path)

-- COMMAND ----------

-- Creates the Tables

DROP TABLE IF EXISTS Q2_Output;
CREATE TABLE Q2_Output
USING CSV
OPTIONS (Path "dbfs:/FileStore/tables/Q2_Output.csv", Header "False", InfersSchema "true");

DROP TABLE IF EXISTS Q3_Output;
CREATE TABLE Q3_Output
USING CSV
OPTIONS (Path "dbfs:/FileStore/tables/Q3_Output.csv", Header "False", InfersSchema "true");

DROP TABLE IF EXISTS Q4_Output;
CREATE TABLE Q4_Output
USING CSV
OPTIONS (Path "dbfs:/FileStore/tables/Q4_Output.csv", Header "False", InfersSchema "true");

DROP TABLE IF EXISTS Q5_Output;
CREATE TABLE Q5_Output
USING CSV
OPTIONS (Path "dbfs:/FileStore/tables/Q5_Output.csv", Header "False", InfersSchema "true");

DROP TABLE IF EXISTS FA1_Output;
CREATE TABLE FA1_Output
USING CSV
OPTIONS (Path "dbfs:/FileStore/tables/FA1_Output.csv", Header "False", InfersSchema "true");

DROP TABLE IF EXISTS FA2_Output;
CREATE TABLE FA2_Output
USING CSV
OPTIONS (Path "dbfs:/FileStore/tables/FA2_Output.csv", Header "False", InfersSchema "true");

DROP TABLE IF EXISTS FA3_Output;
CREATE TABLE FA3_Output
USING CSV
OPTIONS (Path "dbfs:/FileStore/tables/FA3_Output.csv", Header "False", InfersSchema "true");

-- COMMAND ----------

-- Checks the Output Tables

SHOW TABLES LIKE 'fa*|q*';

-- COMMAND ----------

-- Checks the Content of Question 2

SELECT *
FROM Q2_OUTPUT;

-- COMMAND ----------

-- Checks the Content of Question 3

SELECT *
FROM Q3_OUTPUT;

-- COMMAND ----------

-- Checks the Content of Question 4

SELECT *
FROM Q4_OUTPUT;

-- COMMAND ----------

-- Checks the Content of Question 5

SELECT *
FROM Q5_OUTPUT;

-- COMMAND ----------

-- Checks the Content of Further Analysis 1

SELECT *
FROM FA1_OUTPUT;

-- COMMAND ----------

-- Checks the Content of Further Analysis 2

SELECT *
FROM FA2_OUTPUT;

-- COMMAND ----------

-- Checks the Content of Further Analysis 3

SELECT *
FROM FA3_OUTPUT;

-- COMMAND ----------

-- Outputs Imported to Power Bi
