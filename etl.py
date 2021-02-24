import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf ,col,year,month,dayofmonth,hour,weekofyear,date_format
from pyspark.sql.types import StructType as R, StructField as Fld,  DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as long
import pyspark.sql.functions as F
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create the spark session , entry point of the program
    """
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    return spark

def read_data(spark,input_data,format='csv',columns='*',test_size=None,**options):
    """[summary]
    This functions reads different data format from spark. 
    Args:
        spark ([obj]): [spark session]
        input_data ([String]): [path to the source data to read]
        format (str, optional): [file format to read]. Defaults to 'csv'.
        columns (str, optional): [columns to read, subset or all columns]. Defaults to '*'.
        test_size ([integer], optional): [Number of row to return for debuging purpose]. Defaults to None.
    """

    if test_size is None:
        df  = spark.read.load(input_data,format=format,**options).select(columns).drop_duplicates()
    else:
        df  = spark.read.load(input_data,format=format,**options).select(columns).drop_duplicates().show(test_size)
    return df


def save_data(df,output_path,mode="overwrite",out_format='parquet',columns='*',partitionBy=None,**options):
    """[summary]
    This functions save a file to a specified path after it has been processed 
    Args:
        df ([object]): [dataframe to be saved]
        output_path ([type]): [destination path where the file will be saved]
        mode (str, optional): [mode of saving, overwrite to replace the file if already exists]. Defaults to "overwrite".
        out_format (str, optional): [format of the ouput file]. Defaults to 'parquet'.
        columns (str, optional): [column list to save]. Defaults to '*'.
        partitionBy ([type], optional): [describe columns to partition the data by]. Defaults to None.
    """

    df.write.save(output_path,mode=mode,format=out_format,partitionBy=partitionBy,**options)


def process_airport_data(spark,input_data,output_data,test_size=None,columns='*',**options):
    """[summary]
    This function process airport data from S3 and extract 
    relevant information
    Args:
        spark ([object]): [sparksession, entry point of the program]
        input_data ([String]): [path where the data is stored]
        output_data ([type]): [destination folder where the final table will be stored]
    """
    
    # useful columns for our project
    columns=['ident','type','name','elevation_ft','municipality','gps_code','local_code']
    
    #reading data
    airport_df = read_data(spark,input_data,format='csv',columns=columns,test_size=test_size,**options)
    #print(airport_df)
    # saving data
    save_data(airport_df,output_path,mode="overwrite",out_format='parquet',columns=columns,**options)


def process_demographic_data(spark,input_data,output_path,test_size=None,**options):
    """[summary]
    This function process us_demographic data from S3 and extract 
    relevant information
    Args:
         spark ([object]): [sparksession, entry point of the program]
        input_data ([String]): [path where the data is stored]
        output_data ([type]): [destination folder where the final table will be stored]
    """
     #reading data
    demographic_df = read_data(spark,input_data,format='csv',columns='*',test_size=test_size,**options)
    
    #renaming columns
    demographic_df = demographic_df.withColumnRenamed('Median Age','median_age').withColumnRenamed('Male Population','male_population')\
                                   .withColumnRenamed('Female Population','Female_population')\
                                   .withColumnRenamed('Total Population','total_population')\
                                   .withColumnRenamed('Number of Veterans','num_veterans')\
                                   .withColumnRenamed('Average Household Size','avg_household_size')\
                                   .withColumnRenamed('State Code','state_code')\
                                   .withColumnRenamed('Race','race')\
                                   .withColumnRenamed('Count','count')\
                                   .withColumnRenamed('City','city')\
                                   .withColumnRenamed('State','state')

    columns=['state_code','state','city','median_age','male_population','Female_population','total_population','num_veterans',\
        'foreign_born','avg_household_size','race','count']


    # saving data
    save_data(demographic_df,output_path,mode="overwrite",out_format='parquet',columns=columns,**options)





def main():
    spark = create_spark_session()
    #input_data="s3a://thim-bucket-2022/data/airport-codes_csv.csv"
    #output_path="s3a://immigration-schema/airport.parquet"
    input_data="./data/airport-codes_csv.csv" 
    output_path="./data/airport.parquet"
    #process_airport_data(spark,input_data,output_path,test_size=None,header=True)

    input_data="./data/us-cities-demographics.csv" 
    output_path="./data/demographic.parquet"
    process_demographic_data(spark,input_data,output_path,test_size=None,header=True,delimiter=';')
    


if __name__=='__main__':
    main()

    

    spark.stop()

