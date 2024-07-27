import logging
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from utils.logging_config import setup_logging
from utils.validate_json_schema import validate_jsonl
from utils.read_pyspark import JSONDataProcessor

from src.score_images import ImageProcessor



## test yousra scoring logic
from src.yousra_score_images import ImageScorer

# Setup logging configuration
setup_logging()
logger = logging.getLogger(__name__)

class DataValidator:
    """Class to validate JSONL files against their schemas."""
    def __init__(self, files_with_schemas):
        self.files_with_schemas = files_with_schemas

    def validate(self):
        for file_path, schema in self.files_with_schemas:
            try:
                validate_jsonl(file_path=file_path, schema=schema, transfer_invalid=False)
                logger.info(f"File {file_path} validated successfully against schema {schema}.")
            except Exception as ex:
                logger.error(f"Validation failed for {file_path}: {ex}")
                raise

class DataLoader:
    """Class to load JSONL files into DataFrames."""
    def __init__(self, spark):
        self.spark = spark
        self.df_loader = JSONDataProcessor(spark)

    def load_images(self, images_dir):
        return self.df_loader.load_images(images_dir)

    def load_image_tags(self, image_tags_dir):
        return self.df_loader.load_images_tags(image_tags_dir)

    def load_main_images(self, main_images_dir):
        return self.df_loader.load_main_images(main_images_dir)



class ImageTagProcessor:
    """Main class to process image and tag datasets."""
    def __init__(self, spark, images_dir, image_tags_dir, main_images_dir, images_dir_schema, image_tags_dir_schema, main_images_dir_schema, join_col_name="image_id"):
        self.spark = spark
        self.images_dir = images_dir
        self.image_tags_dir = image_tags_dir
        self.main_images_dir = main_images_dir
        self.images_dir_schema = images_dir_schema
        self.image_tags_dir_schema = image_tags_dir_schema
        self.main_images_dir_schema = main_images_dir_schema
        self.join_col_name = join_col_name

    def process(self):
        try:
            # Validate files
            validator = DataValidator([
                (self.images_dir, self.images_dir_schema),
                (self.image_tags_dir, self.image_tags_dir_schema),
                (self.main_images_dir, self.main_images_dir_schema)
            ])
            validator.validate()

            # Load data
            loader = DataLoader(self.spark)
            df_imgs = loader.load_images(self.images_dir).toPandas().to_csv("images.csv")
            df_tags = loader.load_image_tags(self.image_tags_dir).toPandas().to_csv("images_tags.csv")
            df_main_imgs = loader.load_main_images(self.main_images_dir).toPandas().to_csv("main_images.csv")

            # Join data
            df_imgs_tags = df_imgs.join(df_tags, [self.join_col_name] , "left" )

            # Aggregate data to get the maximum probability for each image
            # Define window specification
            window_spec = Window.partitionBy("image_id").orderBy(F.col("probability").desc())

            # Rank rows within each partition
            df_ranked = df_imgs_tags.withColumn("rank", F.row_number().over(window_spec))

            # Filter rows to get only the highest-ranked rows for each image_id
            df_filtered = df_ranked.filter(F.col("rank") == 1).drop("rank")


            # Score images
            image_score = ImageProcessor(spark )
            
    
            df_images_scores = image_score.process_images(df_filtered)#.toPandas().to_csv("output/images_scores.csv")


            # exclude not active and deleted images from main
            ## deleted images are images in main that is not foung in images 
            df_main_tags = df_main_imgs.join(df_imgs , ["hotel_id" , "image_id"] , "inner").join(df_tags, [self.join_col_name] , "left" )




            # Score main images
            df_main_images_scores = image_score.process_images()#.toPandas().to_csv("output/main_images_scores.csv")


            # Compare results of main images 


            # Update main images 


            # Extract metrics 



        except Exception as ex:
            logger.error(f"An error occurred: {ex}")
            raise

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("JoinWithDirectory") \
        .getOrCreate()


    # Get Arguments from CLI
    parser = argparse.ArgumentParser(description="Process and join image and tag datasets, generate CDC, snapshot, and metrics.")
    parser.add_argument('--images', type=str, required=True, help='Path to the images JSONL file.')
    parser.add_argument('--tags', type=str, required=True, help='Path to the image tags JSONL file.')
    parser.add_argument('--main_images', type=str, required=True, help='Path to the main images JSONL file.')
    # parser.add_argument('--output_cdc', type=str, required=True, help='Path to write CDC JSONL file(s).')
    # parser.add_argument('--output_snapshot', type=str, required=True, help='Path to write snapshot JSONL file(s).')
    # parser.add_argument('--output_metrics', type=str, required=True, help='Path to write metrics JSONL file.')

    args = parser.parse_args()
    
    # Define paths and schemas
    images_dir = args.images #'data/images.jsonl'
    image_tags_dir = args.tags #'data/image_tags.jsonl'
    main_images_dir = args.main_images #'data/main_images.jsonl'
    images_dir_schema = 'schemas/image.json'
    image_tags_dir_schema = 'schemas/image_tags.json'
    main_images_dir_schema = 'schemas/main_image.json'
    join_col_name = "image_id"

    # Create the processor and run the main processing function
    processor = ImageTagProcessor(spark,
                                  images_dir,
                                  image_tags_dir,
                                  main_images_dir, 
                                  images_dir_schema, 
                                  image_tags_dir_schema, 
                                  main_images_dir_schema, 
                                  join_col_name)
    df = processor.process()

    # Stop Spark session
    spark.stop()
