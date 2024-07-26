from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col
from typing import Optional, Union

class JSONDataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_json(self, path: str) -> DataFrame:
        """
        Load JSON data into a DataFrame.

        Parameters:
        - path: Path to the JSON file or directory

        Returns:
        - DataFrame containing the JSON data
        """
        return self.spark.read.json(path)
    
    def load_images(self , path):
        return self.load_json( path)
    
    def load_images_tags(self, path: str) -> DataFrame:
        """
        Process and flatten image tags JSON data.

        Parameters:
        - path: Path to the images tags JSON file

        Returns:
        - DataFrame with flattened image tags
        """
        df = self.load_json(path)
        return (
            df.withColumn("tags", explode("tags"))
              .select(
                  col("image_id"),
                  col("tags.tag").alias("tag"),
                  col("tags.version").alias("version"),
                  col("tags.probability").alias("probability")
              )
        )

    def load_main_images(self, path: str) -> DataFrame:
        """
        Process and flatten main images JSON data.

        Parameters:
        - path: Path to the main images JSON file

        Returns:
        - DataFrame with flattened main images data
        """
        df = self.load_json(path)
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
    processor = JSONDataProcessor(spark)
    
    # Load and process data
    images_df = processor.load_json("path/to/images.jsonl")
    tags_df = processor.process_images_tags("path/to/images_tags.jsonl")
    main_images_df = processor.process_main_images("path/to/main_images.jsonl")
    
    # Show the resulting DataFrames
    images_df.show(truncate=False)
    tags_df.show(truncate=False)
    main_images_df.show(truncate=False)

    # Stop SparkSession
    spark.stop()