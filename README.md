# Creating Data Lake Using Apache Spark
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
- calculating the average stay for tourists
- Busiest airport ranking etc.. 

## Explore and Assess the Data

### immigration Dataset
![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/immigration_summary.PNG)
We are going to explore as subset of immigration data use in the analyis is 12 months of data for the year 2016.  We can see the distribution of the data in different months on the figure below:
![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/immigration_count.PNG)
- The field **I94ADDR** is the state destination of the tourist, we see that that column has about 152 592 missing values
- We see that **i94mode** which is the mode by which the tourist entered the USA territory has missing values as well.
- The occupation of our tourists is almost unknown with only 8126.
- We see that the **cicid** column that has information about each tourist is Unique , so there is no duplicated data

### Demographic Dataset
![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/demographic_summary.PNG)
This dataset has information about the distribution of the population in different USA city by race.  The file has about 2891 rows, and has only few missing values as we can see on the image above.

### Airport DataSet
![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/airport_summary.PNG)
As we can see on the immage above, this file has about 55075 rows of diffent airport code in the USA and their coordinates.

## Data Model

The immigration data is main table for this model. We can link to the airport table using state code. We can also link immigration to demographic using state code as well , then derive a date table which is the set of all dates we have on file.
We this level of details, we should be able to answer different questions regarding ethinicity or frequency of tourists by airport or states.

![image](https://raw.githubusercontent.com/tmbothe/ETL-FROM-S3-To-Redshift-using-Spark/main/images/immigration_model.png)


 ## Project Structure
 ```
 The project has two main files, here is the description:
   Data-Lake-with-Apache-Spark
    |
    |   data
    |      | airport-codes_csv.csv
    |      | us-cities-demographics.csv
    |      | 
    |   images
    |      | airport_summary.PNG
    |      | demographic_summary.PNG
    |      | immigration_count.PNG      
    |      | immigration_model.png
    |      | immigration_summary.PNG
    |   etl.py
    |   etl.ipynb
    |   dl.cfg
    |   README.md
 ``` 

   1 - `etl.py` : This is the entry point of the program. Running this script will read the raw files, transform and write the parquet files to the specified output.<br>
   
## Installation 

- Install [python 3.8](https://www.python.org)
- Install [Apache pyspark](hhttps://pypi.org/project/pyspark/)
- Clone the current repository. 
- Create IAM user in AWS and get the user access key and secret key.
- Fill the dl.cfg with the access and secret key