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
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    return spark

#udf to convert sas date
sas_date_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))

def convert_sas_date(df, cols):
    """
      Convert date from SAS date (number of days since 1/1/1960) to datetime 
      :param date: string, timedelta, list, tuple, 1-d array, or Series of SAS date(s) 
      :return: corresponding datetime date(s) 
    """
    for c in [c for c in cols if c in df.columns]:
        df = df.withColumn(c, sas_date_udf(df[c]))
    return df

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


def process_immigration_data(spark,input_data,format='csv',mode="overwrite",columns='*',test_size=None,**options):
    
    df_spark =spark.read.format('com.github.saurfang.sas.spark').load(input_data)

    #convert sas date to date
    cols =['arrdate','depdate']
    df_spark = convert_sas_date(df_spark,cols)
   
    df_spark = df_spark.withColumn("arrdate",df_spark['arrdate'].cast(DateType()))
    df_spark = df_spark.withColumn("depdate",df_spark['depdate'].cast(DateType()))


    columns = ['tourist_id','year','month','i94cit','i94res','port_arrival','arrival_date','arrival_mode','entry_state',
                'departure_date','age','visa_code','count','register_date','visa_post','arrival_flag','departure_flag','status_flag',
                'match_flag','birth_year','dtaddto','gender','airline_code','admission_num','flight_num','vista_type']
    

    #renaming columns
    df_spark = df_spark.withColumnRenamed('cicid','tourist_id'). \
    withColumnRenamed('i94yr','year'). \
    withColumnRenamed('i94mon','month'). \
    withColumnRenamed('i94cit','i94cit'). \
    withColumnRenamed('i94res','i94res'). \
    withColumnRenamed('i94port','port_arrival'). \
    withColumnRenamed('arrdate','arrival_date'). \
    withColumnRenamed('i94mode','arrival_mode'). \
    withColumnRenamed('i94addr','entry_state'). \
    withColumnRenamed('depdate','departure_date'). \
    withColumnRenamed('i94bir','age'). \
    withColumnRenamed('i94visa','visa_code'). \
    withColumnRenamed('dtadfile','register_date'). \
    withColumnRenamed('visapost','visa_post'). \
    withColumnRenamed('entdepa','arrival_flag'). \
    withColumnRenamed('entdepd','departure_flag'). \
    withColumnRenamed('entdepu','status_flag'). \
    withColumnRenamed('matflag','match_flag'). \
    withColumnRenamed('biryear','birth_year'). \
    withColumnRenamed('dtaddto','dtaddto'). \
    withColumnRenamed('gender','gender'). \
    withColumnRenamed('airline','airline_code'). \
    withColumnRenamed('admnum','admission_num').\
    withColumnRenamed('fltno','flight_num').\
    withColumnRenamed('visatype','vista_type')

    df_spark=df_spark.select(columns)

    # saving data
    save_data(df_spark,output_path,mode=mode,out_format='parquet',columns=columns,**options)
    
    
def process_all_files(input_path,output_path):
    
    sas_files = [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith('.sas7bdat')]
    
    for file in sas_files:
        try:
            #tif spark.read.load(output_path).count() >0:
            process_immigration_data(spark,file,mode="append",format='sas7bdat',partitionBy=["year","month"],columns='*',test_size=None)
        except:
            process_immigration_data(spark,file,mode="overwrite",format='sas7bdat',partitionBy=["year","month"],columns='*',test_size=None)
   
  
def process_time_table(spark,immigration_path,time_output):
    df = spark.read.parquet(immigration_path)
    arrival_date= df.withColumn("arrival_date",df['arrival_date'].cast(DateType())).distinct()
    departure_date= df.withColumn("departure_date",df['departure_date'].cast(DateType())).distinct()
    time_table = arrival_date.union(departure_date)
    
    # write time table to parquet files partitioned by year and month
    time_table = df.select('arrival_date').withColumn('day',dayofmonth('arrival_date'))
    time_table = time_table.withColumn('week',weekofyear('arrival_date'))
    time_table = time_table.withColumn('month',month('arrival_date'))
    time_table = time_table.withColumn('year',year('arrival_date'))
    time_table = time_table.withColumn("dayofweek", dayofweek('arrival_date'))
    time_table=time_table.dropDuplicates()
    time_table=time_table.withColumnRenamed('arrival_date', 'date')
    # write time table to parquet files partitioned by year and month
    # saving data
   
    save_data(time_table,time_output,mode="overwrite",out_format='parquet',partitionBy=["year","month"])
    


def main():
    spark = create_spark_session()
    
    #process airport files
    input_data="./data/airport-codes_csv.csv" 
    output_path="./data/airport.parquet"
    process_airport_data(spark,input_data,output_path,test_size=None,header=True)
    
    #Process demographic file
    input_data="./data/us-cities-demographics.csv" 
    output_path="./data/demographic.parquet"
    process_demographic_data(spark,input_data,output_path,test_size=None,header=True,delimiter=';')
    
    #Process immingration file
    input_path = '../../data/18-83510-I94-Data-2016'
    output_path="./data/immigration.parquet"
    process_all_files(input_path,output_path)

    #Process time
    immigration_path ="./data/immigration.parquet"
    time_output= "./data/time.parquet"
    process_time_table(spark,immigration_path,time_output)



if __name__=='__main__':
    main()

    

    spark.stop()

