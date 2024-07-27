from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col
from typing import Optional, Union

from utils.read_json import JSONFileProcessor

class JSONpysparkreader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.json_processor = JSONFileProcessor()

    def load_json(self, path: str , schema_path) -> DataFrame:
        """
        Load JSON data into a DataFrame.

        Parameters:
        - path: Path to the JSON file or directory

        Returns:
        - DataFrame containing the JSON data
        """

        data = self.json_processor.read_jsonl(path , schema_path)

        df = self.spark.createDataFrame(data)

        return df
    
    def load_images(self , path , schema_path):
        return self.load_json( path, schema_path)
    
    def load_images_tags(self, path: str , schema_path) -> DataFrame:
        """
        Process and flatten image tags JSON data.

        Parameters:
        - path: Path to the images tags JSON file

        Returns:
        - DataFrame with flattened image tags
        """
        df = self.load_json(path,   schema_path)
        return (
            df.withColumn("tags", explode("tags"))
              .select(
                  col("image_id"),
                  col("tags.tag").alias("tag"),
                  col("tags.version").alias("version"),
                  col("tags.probability").alias("probability")
              )
        )

    def load_main_images(self, path: str , schema_path) -> DataFrame:
        """
        Process and flatten main images JSON data.

        Parameters:
        - path: Path to the main images JSON file

        Returns:
        - DataFrame with flattened main images data
        """
        df = self.load_json(path , schema_path)
        return (
            df.select(
                col("key.hotel_id").alias("hotel_id"),
                col("value.image_id").alias("image_id"),
                col("value.cdn_url").alias("cdn_url")
            )
        )

  

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SOLID JSONL Processing") \
        .getOrCreate()

    # Initialize the JSONDataProcessor
    processor = JSONpysparkreader(spark)
    
    # processor.load_json("data/main_images.jsonl" ,"schemas/main_image.json")

    # Load and process data
    images_df = processor.load_json("data/images.jsonl" , "schemas/image.json")
    tags_df = processor.load_images_tags("data/image_tags.jsonl" , "schemas/image_tags.json")
    main_images_df = processor.load_main_images("data/main_images.jsonl", "schemas/main_image.json")
    
    # Show the resulting DataFrames
    images_df.show(truncate=False)
    tags_df.show(truncate=False)
    main_images_df.show(truncate=False)

    # # Stop SparkSession
    # spark.stop()