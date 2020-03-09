# LendingClubDataPipeline

**Description**
LendingClub is an American peer-to-peer lending company, headquartered in San Francisco, California. It was the first peer-to-peer lender to register its offerings as securities with the Securities and Exchange Commission, and to offer loan trading on a secondary market. 

**Data Source**

Sample data This dataset contain complete loan data for all loans issued through the 2007-2015, including the current loan status (Current, Late, Fully Paid, etc.) and latest payment information.
 downloaded the dataset in CSV format (loan.csv) and associated dictionary (LCDataDictionary.xlsx) from
https://www.kaggle.com/wendykan/lending-club-loan-data.

## Exploratory data analysis
TBD...

## Data Pipeline Application
**Tools/technogies** 

1. Spark: Big data processing 
2. Java: pipeline application development 
3. PySpark/Databricks: for exploratory data analysis
4. Parquet: file format to store cleaned data
5. Postgres: relational database to store cleaned data

**Pipeline steps**

1. Load row source data (in csv) into Spark
2. Feed the source data to data filter/format/clean pipeline
3. Write the cleaned data to Parquet file
4. Write the Parquet file content to Postgres database table

**Application/program flow description**

The main entry point of the application is the DataPipelineApp class. This class reads user arguments into an Options object. It creates SparkSession using spark options passed as argument to the application. The class uses two other classes to do data cleaning and persistance to databaase (DataTransformer and DataPersister respectively). The role of the DataTransformer class is to load source data as spark Dataset, run a pipe of filtering/formating/cleaning/filling actions on selected subset of the data, and write the resulting data to Parquet. The DataPersister class is responsible to consume the Parquet file and write to database.







