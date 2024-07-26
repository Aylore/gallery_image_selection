from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType
import logging
import math
from datetime import datetime

# Setup logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MIN_RES = 160000
MAX_RES = 2073600
MIN_AR = 0.3
MAX_AR = 4.65
MAX_FRESHNESS_DAY = 10 * 365

WEIGHT_RESOLUTION = 6
WEIGHT_ASPECT_RATIO = 2
WEIGHT_FRESHNESS = 2
WEIGHT_TAG_PRIORITY = 3

class ImageProcessor:
    """Class for processing and scoring images."""

    def __init__(self, spark, images_df):
        self.spark = spark
        self.images_df = images_df

    def process_images(self):
        # Convert created_at to timestamp
        images_df = self.images_df.withColumn("created_at", F.col("created_at").cast("timestamp"))

        # Calculate features
        images_df = images_df \
            .withColumn("resolution", F.col("width") * F.col("height")) \
            .withColumn("aspect_ratio", F.when(F.col("height") == 0, None).otherwise(F.col("width") / F.col("height"))) \
            .withColumn("freshness_days", F.datediff(F.current_date(), F.col("created_at"))) \
            .withColumn("score_resolution", F.when(F.col("resolution") <= 0, 0)
                                             .otherwise(F.least(F.log(F.col("resolution") / MIN_RES) / F.log(F.lit((MAX_RES) / MIN_RES)), F.lit(1)))) \
            .withColumn("score_aspect_ratio", F.when(F.col("aspect_ratio").isNull(), 0)
                                               .otherwise(F.when((F.col("aspect_ratio") >= MIN_AR) & (F.col("aspect_ratio") <= MAX_AR), F.lit(1))
                                                           .otherwise(0))) \
            .withColumn("score_freshness", F.when(F.col("freshness_days") > MAX_FRESHNESS_DAY, F.lit(0))
                                             .otherwise(F.least(1 - (F.col("freshness_days") / MAX_FRESHNESS_DAY), F.lit(1)))) \
            .withColumn("score_tag_priority", F.col("probability")) \
            .withColumn("image_score", (WEIGHT_RESOLUTION * F.col("score_resolution") +
                                         WEIGHT_ASPECT_RATIO * F.col("score_aspect_ratio") +
                                         WEIGHT_FRESHNESS * F.col("score_freshness") +
                                         WEIGHT_TAG_PRIORITY * F.col("score_tag_priority")) / 
                                        (WEIGHT_RESOLUTION + WEIGHT_ASPECT_RATIO + WEIGHT_FRESHNESS + WEIGHT_TAG_PRIORITY))

        # Rank images within each hotel_id
        window_spec = Window.partitionBy("hotel_id").orderBy(F.col("image_score").desc())
        images_df = images_df.withColumn("rank", F.row_number().over(window_spec))
        
        # Select the highest scored image for each hotel_id
        images_df = images_df.filter(F.col("rank") == 1).drop("rank")

        return images_df

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ImageProcessing") \
        .getOrCreate()

    # Define schema
    images_schema = StructType([
        StructField("image_id", StringType(), True),
        StructField("cdn_url", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("hash", StringType(), True),
        StructField("height", IntegerType(), True),
        StructField("hotel_id", IntegerType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("width", IntegerType(), True),
        StructField("tag", StringType(), True),
        StructField("version", StringType(), True),
        StructField("probability", FloatType(), True)
    ])

    # Load data
    images_df = spark.read.schema(images_schema).json("path_to_images_jsonl")

    # Process images
    processor = ImageProcessor(spark, images_df)
    result_df = processor.process_images()

    # Show results
    result_df.show(truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
