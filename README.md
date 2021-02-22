# ETL-from-S3-To-Redshift-using-Spark
In this project, we are going to use boto3 sdk to load files from our local environment to AWS s3, then create a Data Lake using apache Spark.

## Scope the Project and Data Gathering

The project is built with data coming from 3 different sources:
- **I94 Immigration Data:** This data comes from the US National Tourism and Trade Office here(https://travel.trade.gov/research/reports/i94/historical/2016.html). The dataset has information about tourists coming to the USA. Where they are coming from, their age, the length of their stay, the type of visa etc..
- **U.S. City Demographic Data:** This dataset has information about US population in different state by  race, gender .
- **Airport Code Table:** This is the table of airport codes and corresponding cities. It comes from here(https://datahub.io/core/airport-codes#data).

The purpose of the project is to analyze the tourists frequency in the USA by  month and answer the following questions:
- Analyze in which month we should expect more tourists
- State ranking for tourists destination
- Correlate the origin of tourists with USA population by race
- Busiest airport ranking etc.. 

## Explore and Assess the Data

### immigration Dataset
![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/immigration_summary.PNG)
We are going to explore as subset of immigration data for april 2016.  As we can see on the picture above, it has 3 096 313 rows and columns. Here are some observations:
- The field **I94ADDR** is the state destination of the tourist, we see that that column has about 152 592 missing values
- We see that **i94mode** which is the mode by which the tourist entered the USA territory has missing values as well.
- The occupation of our tourists is almost unknown with only 8126 values out of  3 096 313
- We see that the **cicid** column that has information about each tourist is Unique , so there is no duplicated data

### Demographic Dataset
![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/demographic_summary.PNG)
This dataset has information about the distribution of the population in different USA city by race.  The file has about 2891 rows, and has only few missing values as we can see on the image above.

### Airport DataSet
![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/airport_summary.PNG)
As we can see on the immage above, this file has about 55075 rows of diffent airport code in the USA and their coordinates.

## Data Model

![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/immigration_model.png)