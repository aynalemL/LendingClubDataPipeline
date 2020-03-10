# LendingClubDataPipeline

**OverView**
LendingClub is an American peer-to-peer lending company, headquartered in San Francisco, California. It was the first peer-to-peer lender to register its offerings as securities with the Securities and Exchange Commission and to offer loan trading on a secondary market. 

**Data Source**

Sample data This dataset contains complete loan data for all loans issued through 2007-2015, including the current loan status (Current, Late, Fully Paid, etc.) and latest payment information.
 Downloaded the dataset in CSV format (loan.csv) and associated dictionary (LCDataDictionary.xlsx) from
https://www.kaggle.com/wendykan/lending-club-loan-data.

## Exploratory data analysis

## Overview
The file was uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows storing data for querying inside of Databricks.

The exploratory data anlytics was conducted in a databricks notebook is written in **Python**. Signup for Databricks community version here-https://databricks.com/signup/signup-community.

## Assumption and Steps**
**Assumption**
1. Since the main purpose of this exploratory data analysis is to understand the data and exploration and preparation for further analytics, I assumed that it is essential to dropping some of the columns more than 25% null/missing values.

2. For the pipeline, I didn't remove the column with even if the value is 100% null,  since the data science team may want to use it by   generating the value or refactoring it. 

**steps to explore the data in details:**
1. upload the file into Databrick DBFS(Databrick File Storage service and read from there using pyspark.
2. print the schema to identify the data type and check the patterns
3. Since it makes the process slow, I selected a sample of the data to do data quality analysis. using random sampling I selected 100,000. 
4. Count the number of null in each column and find a list of columns that have more than 25% null values, and drop the column with more than 25% null values
5. To properly examine each column, load sample column only 10 columns at a time and identify the pattern, check data quality visualize and do a descriptive stat to understand the nature of the data and identify data quality issues that need fixed/ cleaning.   For example, a column such as annual _inc was string while the correct data type is numeric, column-like terms have additional string months which will be a problem to do numerical statistic, etc... 

For more details please see the notebook(Databrics Notebook) in the scr code.

Note: The results of this data exploration was used to create an application that clean, format and validate the data for the pipeline. he value. Plus, where deemed necessary and when the missing values are not more than 25%, I created a method that fills it with the average of the column.

## Data Pipeline Application
**Tools/technogies** 

1. Spark: Big data processing 
2. Java: pipeline application development
3. Junit: unit testing 
4. PySpark/Databricks: for exploratory data analysis
5. Parquet: file format to store cleaned data
6. Postgres: relational database to store cleaned data

**Pipeline steps**

1. Load row source data (in CSV) into Spark
2. Feed the source data to the data filter/format/clean pipeline
3. Write the cleaned data to Parquet file
4. Write the Parquet file content to Postgres database table

**Application/program flow description**

The main entry point of the application is the DataPipelineApp class. This class reads user arguments into an Options object. It creates SparkSession using spark options passed as an argument to the application. The class uses two other classes to do data cleaning and persistence to the database (DataTransformer and DataPersister respectively). The role of the DataTransformer class is to load source data as spark Dataset, run a pipe of filtering/formating/cleaning/filling actions on the selected subset of the data, and write the resulting data to Parquet. The DataPersister class is responsible to consume the Parquet file and write to the database.

## Next System Improvement
1. Doing more data cleaning such as explore the option for filling missing values 
2. Create more visualization and statistic to understand the relationship among the different columns
3. Explore option to updating and insert the new data instead of just appending to existing data
4. Create a normalized table (if applicable ).
5. Do further functional and performance testing and optimize the application to be more reusable and maintainable and performant.
5. performance benchmarking to identify opportunities to increase scalability as well as optimize performance 
 
