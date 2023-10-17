# Rolling Correlation Analysis of Stock Market Indices

## Introduction

### What is a Stock Market Index?
- A number calculated based on share prices of a list of important companies
- A representation for the market performance

### What is a Correlation Coefficient?
- A metric to measure the strength of two indices moving in tandem with one another
- Cor. coef. = 1 means two indices will move by the same amount, and in the same direction
- Cor. coef. = -1 means two indices will move by the same amount, but in the opposite direction

### What is a rolling correlation coefficient?
- A rolling correlation, akin to a moving average, is simply the moving correlation coefficient between two time series over a specified window width.
- For example, with a window width of 24 months, the 24-month rolling correlation as at December 2020 is the correlation coefficient between two time series using data from January 2019 until December 2020 (i.e. 24 months).

### Problem Statement & Motivation
- Most publicly available correlation coefficients are static over a specified timeframe, but they can change rapidly (e.g. during financial turmoil)
→ Trend of rolling correlation coefficients for a moving window (e.g. 24 months) could provide additional insights to investors

Limited past studies on stock market indices using big data tools
→ Use big data tools to perform rolling correlation analysis on stock market indices

### Aim
- To perform rolling correlation analysis between stock market indices using big data tools (Apache Hive and Apache PySpark).

## Work FLow
An ETL pipeline will be created using Apache Hive, then the output result will be transferred to Apache PySpark for Rolling Coefficient Analysis.
![image](https://github.com/Zayuki/Rolling_Correlation_Analysis_of_Stock_Market_Indices/assets/67309677/0dbdb895-4272-4726-a002-2e082105bb18)

### Data Acquisition

| Title    | Stock Exchange Data | 
| -------------- | ------------------- |
| Source  | [Cody. (2021). Kaggle. ](https://www.kaggle.com/datasets/mattiuzc/stock-exchange-data)https://www.kaggle.com/datasets/mattiuzc/stock-exchange-data | 
| Period  | Year 1965 till 2021 |
| Description  | Daily price data for indices tracking stock exchanges from all over the world (United States, China, Canada, Germany, Japan, and more). The data was all collected from Yahoo Finance, which had several decades of data available for most exchanges. Prices are quoted in terms of the national currency of where each exchange is located. |
| Filename  | indexData.csv |
| Number of rows  | 112,457 rows |
| Number of columns  | 8 columns |


### ETL Process using Apache Hive
![image](https://github.com/Zayuki/Rolling_Correlation_Analysis_of_Stock_Market_Indices/assets/67309677/0facee6a-692c-4d3b-8a9d-8fa026d1284f)

| Step | Description |
|------|-------------|
| Data Storage | |
| 1    | Save raw dataset (`indexData.csv`) into HDFS. |
| 2    | Load raw dataset into a data processing tool. |
| Data Cleaning | |
| 3    | Count rows before deletion. |
| 4    | Delete rows with missing values. |
| 5    | Count rows after deletion. |
| 6    | Delete "Adj Close" and "Volume" columns. |
| Data Pre-Processing | |
| 7    | Add "Year," "Month," and "Day" columns based on the "Date" column. |
| 8    | Add a "Year-Month" column. |
| Data Transformation for Monthly Data | |
| 9    | For all "Index" names, average all the indices ("Open," "High," "Low," "Close") by "Year-Month." |
| 10   | Query the average monthly "Close" column. |
| 11   | Export the queried result as a CSV file. |

- Data Storage
1) Create external table and table in Apache Hive:
External table:
```SQL
CREATE EXTERNAL TABLE IF NOT EXISTS indexdata_csv(
Index STRING,
Date2 DATE,
Open FLOAT,
High FLOAT,
Low FLOAT,
Close FLOAT,
Adj_Close FLOAT,
Volumne BIGINT)
ROW FORMAT DELIMITTED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hdfs/apachehive'
TBLPROPERTIES ('skip.header.line.count'='1');
```
Internal table:
```SQL
CREATE EXTERNAL TABLE indexdata (
Index STRING,
Date2 DATE,
Open FLOAT,
High FLOAT,
Low FLOAT,
Close FLOAT,
Adj_Close FLOAT,
Volumne BIGINT)
COMMENT 'Stock market indices'
ROW FORMAT DELIMITTED
FIELDS TERMINATED BY ','
STORED AS orc
TBLPROPERTIES ('transactional'='true);
```
2) Save raw dataset (`indexData.csv`) into HDFS.
```SQL
INSERT INTO TABLE indexdata SELECT * FROM indexdata_csv;
```

- Data Cleaning
3) Count rows before deletion
```SQL
SELECT coutn(*) FROM indexdata;
```
4) Delete rows with missing values
```SQL
DELETE FROM indexdata WHERE open is NULL;
```
5) Count rows after deletion
```SQL
SELECT coutn(*) FROM indexdata;
```
6) Delete “Adj Close” and “Volume” columns
```SQL
CREATE TABLE indexdata_tmp AS SELECT Index, Date2, Open, High, Low, Close
FROM indexdata;
```

- Data Preprocessing
7) Add “Year”, “Month”, “Day” columns based on “Date” column
```SQL
CREATE TABLE indexdata_tmp1 AS SELECT *, Year(Date2) as Year, LPAD(MONTH(DATE2),2,0) As Month, DAY(Date2) as Day
FROM indexdata_tmp;
```
8) Add “Year-Month” column
```SQL
CREATE TABLE indexdata_final AS SELECT *, CONCAT(YEAR,'-',MONTH) as YearMonth FROM indexdata_tmp1;
```
9) For all “Index” names, average all the indices (“Open”, “High”, “Low”, “Close”) by “Year-Month”
```SQL
CREATE TABLE indexdata_month AS
SELECT Index, YearMonth, AVG(Open) AS Open, AVG(High) AS High, AVG(Low) AS Low, AVG(Close) as Close
FROM indexdata_final
GROUP BY Index, YearMonth;
```
- Data Transformation for Monthly Data
10) Query average monthly "Close" column.
```SQL
SELECT Index, YearMonth, Close
FROM indexdata_month LIMIT 10;
```
11) Export Queried results as CSV file and import to PySpark for analysis.
```SQL
INSERT OVERWRITE LOCAL DIRECTORY
'file:///home/lxy/Downloads/hive_monthly'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Index, YearMonth, Close
FROM indexdata_month;
```

### Procedure for Rolling Correlation Analysis with PySpark
![image](https://media.licdn.com/dms/image/C4E12AQEb6oxAxtYD-Q/article-cover_image-shrink_600_2000/0/1620420835464?e=2147483647&v=beta&t=qbt9g5HjMukXzDqXW6Y9QonzSkSbVqVm58OWnxcLhlg)

| Step | Description |
|------|-------------|
| 1    | Import the queried result from the data access stage above (CSV) into PySpark. |
| 2    | For ease of analysis, spread rows into different index columns using the "pivot" function. Convert "Year-Month," "Index," and "AvgClose" columns into "Year-Month," "Index0," "Index1," "Index2," ..., "Index13." |
| 3    | Compute pairwise 24-month rolling correlation coefficients for each pair of stock market indices. This involves a total of 91 pairs from 14 stock market indices. |
| 4    | Export all pairwise rolling correlation coefficients into separate CSV files for further analysis and visualization. |

- Step 1: Load Python Libraries in Apache PySpark.
```Python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
```
Import the queried result from Apache Hive (csv) into PySpark
```Python
spark = SparkSession.builder.getorCreate()
Dataschema1= StructType([StructField("Index", StringType(), True),
StructField("YearMonth", StringType(), True),
StructField("AvgClose", FloatType(), True)])

df = spark.read.format("csv").option("header","false").schema(Dataschema1)
load("/home/lxy/Downloads/hive_monthly/000000_0")

df.show(5)
```

- Step 2: For ease of analysis, spread rows into different index columns using the "pivot" function. Convert "Year-Month," "Index," and "AvgClose" columns into "Year-Month," "Index0," "Index1," "Index2," ..., "Index13."
```Python
pivotdf = df.groupBy("YearMonth").pivot("Index").sum("AvgClose")
pivotdf.show(5)
```

- Step 4: Compute pairwise 24-month rolling correlation coefficients for each pair of stock market indices (altogether 91 pairs from 14 stock market indices)
``` Python
listcolumn = ["'000001.SS'","'399001.SZ'","GDAXI", "GSPTE", "HSI", "IXIC", "'J203.JO'", "KS11", "N100", "N225", "NSEI", "NYA", "SSMI", "TWII"]

listcolumnpandas = ["000001.SS","399001.SZ","GDAXI", "GSPTE", "HSI", "IXIC", "'J203.JO'", "KS11", "N100", "N225", "NSEI", "NYA", "SSMI", "TWII"]

#Construct user-defined function “extracttwoindices” to perform the analysis:

def extracttwoindices(indexnumber1, indexnumber2):
    index1 = listcolumn[indexnumber1]
    index2 = listcolumn[indexnumber2]
    index1pandas = listcolumnpandas[indexnumber1]
    index2pandas = listcolumnpandas[indexnumber2]
    dffinal = pivotdf.select("YearMonth", index1, index2).filter(col(index1).isNotNull() & col(index2).isNotNull()).sort("YearMonth")
    dffinalpandas = dffinal.toPandas()
    dffinalpandas['RollingCor12m'] = dffinalpandas[index1pandas].rolling(12).corr(dffinalpandas[index2pandas])
    dffinalpandas['RollingCor24m'] = dffinalpandas[index1pandas].rolling(24).corr(dffinalpandas[index2pandas])
    dffinalpandas['RollingCor36m'] = dffinalpandas[index1pandas].rolling(36).corr(dffinalpandas[index2pandas])
    dffinal1 = spark.createDataFrame(dffinalpandas)
    path = "/home/lxy/Downloads/hive_month/" + str(indexnumber1) + "_" +str(indexnumber2) + ".csv"
    dffinal1.write.option("header","true").csv(path)
    return 'done'

# Use “for” loop and “extracttwoindices” function to compute 91 pairs of
rolling correlation coefficients:

for x in range(0,14):
    for y in range(x+1,14):
            extracttwoindices(x,y)
```
