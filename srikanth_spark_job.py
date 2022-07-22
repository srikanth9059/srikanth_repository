from pyspark.sql.types import DecimalType,StringType
from pyspark.sql import functions as f
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws,col,sha2
	  
spark = SparkSession.builder.appName('Data_Transformation').getOrCreate()

class Data_Transformation():
    
    # The __init__ function is called every time an object is created from a class
    def __init__(self): 
        self.jsonData=self.read_config()
        # Assigning variables for Configuration File Parameters
        
        self.ingest_actives_source = self.jsonData['ingest-Actives']['source']['data-location']
        self.ingest_actives_destination = self.jsonData['ingest-Actives']['destination']['data-location']
        self.ingest_viewership_source = self.jsonData['ingest-Viewership']['source']['data-location']
        self.ingest_viewership_destination = self.jsonData['ingest-Viewership']['destination']['data-location']
        self.transformation_cols_actives = self.jsonData['masked-Actives']['transformation-cols']
        self.transformation_cols_viewership = self.jsonData['masked-Viewership']['transformation-cols']
        self.ingest_raw_actives_source = self.jsonData['masked-Actives']['source']['data-location']
        self.ingest_raw_viewership_source = self.jsonData['masked-Viewership']['source']['data-location']
        self.ingest_raw_actives_Destination = self.jsonData['masked-Actives']['destination']['data-location']
        self.ingest_raw_viewership_Destination = self.jsonData['masked-Viewership']['destination']['data-location']
        self.masking_col_actives= self.jsonData['masked-Actives']['masking-cols']
        self.masking_col_viewership= self.jsonData['masked-Viewership']['masking-cols']        
        self.partition_col_actives= self.jsonData['masked-Actives']['partition-cols']
        self.partition_col_viewership= self.jsonData['masked-Viewership']['partition-cols']
        
        
    #To read the configuration file
    def read_config(self):
        configData = spark.sparkContext.textFile("s3://srikanth-landingzone/config_files/app_configuration.json").collect()
        data       = ''.join(configData)
        jsonData = json.loads(data)
        return jsonData
        
    #Function to read file from landing Zone
    def read_data(self, path):
        df = spark.read.parquet(path)
        return df
    
    #Function to write file to Raw Zone
    def write_data(self, df, path, partition_cols = []):
        if partition_cols:
            df.write.mode("overwrite").partitionBy(partition_cols[0], partition_cols[1]).parquet(path)
        else:
            df.write.mode("overwrite").parquet(path)
        
    #Function to mask critical fields in a file
    def mask_data(self, df, column_list):
        for column in column_list:
            #df = df.withColumn("masked_"+column,f.concat(f.lit('***'),f.substring(f.col(column),4,3)))
            df = df.withColumn("masked_"+column,f.sha2(f.col(column),256))
        return df
    
    #Function to transform some fileds in a file
    def transformation(self, df, cast_dict):
        for key in cast_dict.keys(): 
            if cast_dict[key].split(",")[0] == "DecimalType":
                df = df.withColumn(key, df[key].cast(DecimalType(10, int(cast_dict[key].split(",")[1])))) 
            elif cast_dict[key] == "ArrayType-StringType":
                df.withColumn("key", f.concat(f.col(key), f.lit(",")))
        return df
      

# Creating an object for Data_Transformation class
T = Data_Transformation()
T.read_config()

# read actives and viwership files from Landing Zone
actives_landing_data = T.read_data(T.ingest_actives_source)
viewership_landing_data = T.read_data(T.ingest_viewership_source)

# write actives and viwership files from Landing Zone to Raw Zone
T.write_data(actives_landing_data, T.ingest_actives_destination)
T.write_data(viewership_landing_data, T.ingest_viewership_destination)

# read actives and viwership files from Raw Zone
actives_raw_data = T.read_data(T.ingest_raw_actives_source)
viewership_raw_data = T.read_data(T.ingest_raw_viewership_source)

# masking some fields from actives and viwership files
actives_masked_data = T.mask_data(actives_raw_data, T.masking_col_actives)
viewership_masked_data = T.mask_data(viewership_raw_data, T.masking_col_viewership)

# casting some fields from actives and viwership files
actives_tranform_data = T.transformation(actives_masked_data, T.transformation_cols_actives)
viewership_transform_data = T.transformation(viewership_masked_data, T.transformation_cols_viewership)

# write actives and viwership Transformed data to Staging Zone
T.write_data(actives_tranform_data, T.ingest_raw_actives_Destination, T.partition_col_actives)
T.write_data(viewership_transform_data, T.ingest_raw_viewership_Destination, T.partition_col_viewership)

spark.stop()

