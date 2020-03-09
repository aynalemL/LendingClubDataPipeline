# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Exploratory Data Analysis(EDA)
# MAGIC 
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/loan.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC #sample the data to do data quality analysis. Random samples are supposed to be representative
# MAGIC sample = df.sample(False, 0.005)
# MAGIC sample.cache()
# MAGIC #take copy for cleaning. The original sample is used for exploration
# MAGIC cleaned_sample = sample

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import isnan, when, count, col
# MAGIC total = sample.count()
# MAGIC #count the number of null in each column
# MAGIC stat = sample.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in sample.columns]);
# MAGIC #find list of columns that have more than 75% null values
# MAGIC cols_to_drop = []
# MAGIC for c in stat.columns:
# MAGIC   if(stat.select(c).head()[0])/total > 0.75: 
# MAGIC     cols_to_drop.append(c)
# MAGIC #remove columns that have more than 75% null values from the data    
# MAGIC cleaned_sample  = cleaned_sample.drop(*cols_to_drop)
# MAGIC cleaned_sample.cache()
# MAGIC sample = sample.drop(*cols_to_drop)
# MAGIC sample.cache()

# COMMAND ----------

# MAGIC %python
# MAGIC cleaned_sample.describe()
# MAGIC #since ther are large number of columns explore and clean data for range of columns at a time (10 columns at a time)
# MAGIC #column 0 to 10 cleaning 
# MAGIC sample_cols_0_to_10 = sample.select(sample.columns[0:10])
# MAGIC display(sample_cols_0_to_10)

# COMMAND ----------


#from the above display we can see that columns 'term' and 'emp_length' are supposed to be numbers but are strings.
#lets clean these columns
from pyspark.sql.functions import regexp_extract, regexp_replace
#sample_cols_0_to_10 = sample_cols_0_to_10.withColumn('emp_length_cleaned', regexp_replace('emp_length', 'n/a', 'null'))
#display(sample_cols_0_to_10.select(['emp_length_cleaned']))
sample_cols_0_to_10 = sample_cols_0_to_10.withColumn('term_cleaned',regexp_extract(sample['term'], '[0-9]+',0)).withColumn('emp_length_cleaned', regexp_replace('emp_length', "[^0-9]*", ''))
sample_cols_0_to_10 = sample_cols_0_to_10.drop('term');
sample_cols_0_to_10 = sample_cols_0_to_10.drop('emp_length');
sample_cols_0_to_10.describe().show()


# COMMAND ----------

# MAGIC %python
# MAGIC #Based on the above descriptive statistics for columns 0 to 10, emp_length_cleaned has some empty value (thus, min value is empty). Lets #count how many rows have empty emp_length_cleaned
# MAGIC from pyspark.sql.functions import  coalesce,avg, lit
# MAGIC def col_avg(df, colname):
# MAGIC   return df.select(colname).agg(avg(colname))
# MAGIC 
# MAGIC emp_len_avg = col_avg(sample_cols_0_to_10, 'emp_length_cleaned').first()[0]
# MAGIC 
# MAGIC sample_cols_0_to_10 = sample_cols_0_to_10.withColumn('emp_len_avg', lit("{}".format(emp_len_avg)))
# MAGIC sample_cols_0_to_10 = sample_cols_0_to_10.withColumn('emp_length_cleaned_cleaned', when(col("emp_length_cleaned")=='', int(emp_len_avg)).otherwise(col("emp_length_cleaned")) ) 
# MAGIC sample_cols_0_to_10.groupby(sample_cols_0_to_10.emp_length_cleaned_cleaned).agg(func.count('emp_length_cleaned_cleaned')).show()

# COMMAND ----------

#Let us explore categorical data
import pyspark.sql.functions as func
sample_cols_0_to_10.groupby(sample_cols_0_to_10.grade).agg(func.count('grade')).show()


# COMMAND ----------

import pyspark.sql.functions as func
sample_cols_0_to_10.groupby(sample_cols_0_to_10.sub_grade).agg(func.count('sub_grade')).show(100)


# COMMAND ----------

import pyspark.sql.functions as func
sample_cols_0_to_10.groupby(sample_cols_0_to_10.emp_title).agg(func.count('emp_title')).show(200)


# COMMAND ----------

#column 0 to 10 cleaning 
sample_cols_11_to_20 = sample.select(sample.columns[10:20])
display(sample_cols_11_to_20)

# COMMAND ----------

#from the above display we notice that zip_code is invalid and seems to be no way to fix it. So we remove it
#also the issue_d field is date field but not in a date format. That has to be formated
from pyspark.sql.functions import regexp_extract, regexp_replace,substring, length, col, expr,lit
sample_cols_11_to_20=sample_cols_11_to_20.drop('zip_code')
sample_cols_11_to_20= sample_cols_11_to_20.withColumn('issue_date_cleaned', expr("to_date(issue_d, 'MMM-yyyy')" )) 
#sample_cols_11_to_20=sample_cols_11_to_20.drop('issue_d')
sample_cols_11_to_20.select(['annual_inc']).describe().show()


# COMMAND ----------

sample_cols_11_to_20.groupby(sample_cols_11_to_20.home_ownership).agg(func.count('home_ownership')).show(100)

# COMMAND ----------

sample_cols_11_to_20.groupby(sample_cols_11_to_20.title).agg(func.count('title')).show(100, False)


# COMMAND ----------

sample_cols_0_to_10.describe().show()

# COMMAND ----------

sample_cols_21_to_30 = sample.select(sample.columns[20:30])
display(sample_cols_21_to_30)

# COMMAND ----------

# again we want to convert earliest_cr_line columns to proper date type
from pyspark.sql.functions import regexp_extract, regexp_replace,substring, length, col, expr,lit,to_timestamp,to_date, concat
cleaned_sample= cleaned_sample.withColumn('earliest_cr_line_cleaned', expr("to_date(earliest_cr_line, 'MMM-yyyy')" )) 
cleaned_sample = cleaned_sample.drop('earliest_cr_line')
display(cleaned_sample.select(['earliest_cr_line_cleaned']))




# COMMAND ----------

sample_cols_31_to_40 = sample.select(sample.columns[30:40])
display(sample_cols_31_to_40)


# COMMAND ----------

sample_cols_41_to_50 = sample.select(sample.columns[40:50])
display(sample_cols_41_to_50)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace,substring, length, col, expr,lit,to_timestamp,to_date, concat
cleaned_sample= cleaned_sample.withColumn('last_payment_date_cleaned', expr("to_date(last_pymnt_d, 'MMM-yyyy')" )).withColumn('next_payment_date_cleaned', expr("to_date(next_pymnt_d, 'MMM-yyyy')" )).withColumn('last_credit_pull_date_cleaned', expr("to_date(last_credit_pull_d, 'MMM-yyyy')" ))

cleaned_sample = cleaned_sample.drop('last_pymnt_d','next_pymnt_d','last_credit_pull_d')
display(cleaned_sample.select(['last_credit_pull_date_cleaned','next_payment_date_cleaned','last_credit_pull_date_cleaned']))

# COMMAND ----------

sample_cols_51_to_60 = sample.select(sample.columns[50:60])
display(sample_cols_51_to_60)

# COMMAND ----------

sample_cols_61_to_70 = sample.select(sample.columns[60:70])
display(sample_cols_61_to_70)

# COMMAND ----------

sample_cols_71_to_80 = sample.select(sample.columns[70:80])
display(sample_cols_71_to_80)

# COMMAND ----------

sample_cols_81_to_90 = sample.select(sample.columns[80:90])
display(sample_cols_81_to_90)

# COMMAND ----------

sample_cols_91_to_100 = sample.select(sample.columns[90:100])
display(sample_cols_91_to_100)

# COMMAND ----------

sample_cols_101_to_110 = sample.select(sample.columns[100:110])
display(sample_cols_101_to_110)

# COMMAND ----------

display(cleaned_sample)

# COMMAND ----------


