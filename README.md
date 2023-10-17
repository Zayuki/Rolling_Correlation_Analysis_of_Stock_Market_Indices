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
