import logging
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from utils.logging_config import setup_logging
from utils.validate_json_schema import validate_jsonl



from utils.read_pyspark import JSONpysparkreader

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
    def __init__(self, spark ) :
        self.spark = spark
        self.df_loader = JSONpysparkreader(spark)

    def load_images(self, images_dir ,schema):
        return self.df_loader.load_images(images_dir,schema)

    def load_image_tags(self, image_tags_dir,schema):
        return self.df_loader.load_images_tags(image_tags_dir,schema)

    def load_main_images(self, main_images_dir,schema):
        return self.df_loader.load_main_images(main_images_dir,schema)



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
            # validator = DataValidator([
            #     (self.images_dir, self.images_dir_schema),
            #     (self.image_tags_dir, self.image_tags_dir_schema),
            #     (self.main_images_dir, self.main_images_dir_schema)
            # ])
            # validator.validate()

            # Load data
            loader = DataLoader(self.spark)
            df_imgs = loader.load_images(self.images_dir , self.images_dir_schema )#.toPandas().to_csv("images.csv")
            df_tags = loader.load_image_tags(self.image_tags_dir , self.image_tags_dir_schema)#.toPandas().to_csv("images_tags.csv")
            df_main_imgs = loader.load_main_images(self.main_images_dir  , self.main_images_dir_schema)#.toPandas().to_csv("main_images.csv")

            ############3
            ## SHOW DATA
            # df_imgs.show(20 ,False , True)
            # df_tags.show(20 ,False , True)
            # df_main_imgs.show(20 ,False , True)

            #############

            # Join data
            df_imgs_tags = df_imgs.join(df_tags, [self.join_col_name] , "left" )

            # Aggregate data to get the maximum probability for each image
            # Define window specification
            window_spec = Window.partitionBy("image_id").orderBy(F.col("probability").desc())

            # Rank rows within each partition
            df_ranked = df_imgs_tags.withColumn("rank", F.row_number().over(window_spec))

            # Filter rows to get only the highest-ranked rows for each image_id
            df_filtered = df_ranked.filter(F.col("rank") == 1).drop("rank")
            
            df_filtered_active = df_filtered.withColumn("change_type", 
                                                      F.when(F.col("is_active").cast("boolean")  == False, "inactive")\
                                                        .when(F.col("is_active").cast("boolean").isNull() , "deleted")\
                                                            .otherwise("other")).filter(F.col("change_type") != "inactive")

            # Score images
            image_score = ImageProcessor(spark )
            
    
            df_image_scored = image_score.process_images(df_filtered_active)#.toPandas().to_csv("output/images_scores.csv")







            ## join main images with images 
            df_main_with_images = df_main_imgs.drop( "cdn_url").join(df_imgs.drop("hotel_id" ),
                                                     on= "image_id",
                                                       how= "left")

            ## get image status for each image deleted active not active 
            df_main_with_images =  df_main_with_images.withColumn("change_type", 
                                                      F.when(F.col("is_active").cast("boolean")  == False, "inactive")\
                                                        .when(F.col("is_active").cast("boolean").isNull() , "deleted")\
                                                            .otherwise("other"))


            ## join the images scores with main_images
            # df_main_scored = df_main_with_images.join(df_image_scored.select("hotel_id" ,
            #                                                                   F.col("image_id").alias("old_image_id") ,
            #                                                                   "image_score"),
            #                                            on= "hotel_id",
            #                                              how= "left")

            df_main_scored = df_main_with_images.join(df_image_scored.select(F.col("image_id") ,
                                                                              F.col("image_score").alias("old_image_score")),
                                                       on= "image_id",
                                                         how= "left")


            df_main_scored = df_main_scored.join(df_image_scored.filter(F.col("rank") == 1).drop("rank")\
                                                 .select("hotel_id", F.col("image_id").alias("new_image_id"),
                                                         F.col("image_score").alias("new_image_score")),
                                                                              "hotel_id",
                                                                              "left")



            ## GET updated main_images 

            df_main_images_updated  = df_main_scored.select(F.col("new_image_id").alias("image_id"),
                                                            F.col("new_image_score").alias("image_score"),
                                                            "hotel_id",
                                                            "cdn_url")



            # df_main_scored.toPandas().to_csv("output/main_images_with_the_hotel_max_scored_image_with_old_image_score_inactive_filter.csv")

            # df_main_scored.withColumn("hotel_image_status" , F.when("image_score"))

            ## For change_type `other`  we need to compare the old score with the new image score






            ## join main images with image scored to get the hotels in images that doesnt exist in main_images
            df_new_hotels= df_image_scored.join(df_main_imgs, on = "hotel_id", how= "left_anti")


            hotels_with_images = df_imgs.join(df_main_imgs.drop("hotel_id" , "cdn_url"), on="image_id")\
                .filter(F.col("is_active").cast("boolean") != False)\
                    .select("hotel_id").distinct()
            
            # hotels_with_images.show(20 , False , True)


            deleted_main_images = df_main_with_images.filter(F.col("change_type") == "deleted").count()

            # print(deleted_main_images)


            # Compare results of main images 


            # Update main images 


            # Extract metrics 
            no_images_processed = df_image_scored.select("image_id").distinct().count()

            df_main_scored = df_main_scored.withColumn(
                "updated",
                F.when(F.col("change_type") == "deleted", True)
                .when(F.col("change_type") == "inactive", True)
                .when((F.col("change_type") == "other") & (F.col("image_id") != F.col("new_image_id")), True)
                .otherwise(False)
)           
            df_main_scored.groupBy("updated").count().show()


            df_imgs.select(F.col("is_active").cast("boolean")).distinct().show()

            hotels_with_images = df_imgs.drop("hotel_id").join(df_main_imgs, on="image_id") \
                .filter(F.col("is_active").cast("boolean") != False) \
                .select("hotel_id").distinct()

            df_new_hotels = df_image_scored.join(df_main_imgs, on="hotel_id", how="left_anti")




            df_transformed = df_main_images_updated.select(
                  F.struct(F.col("hotel_id").alias("hotel_id")).alias("key"),
                    F.struct(
                        F.col("image_id").alias("image_id"),
                        F.col("cdn_url").alias("cdn_url")
                    ).alias("value")
                )

            df_transformed.write.mode('overwrite').json("output/updated_main_images.jsonl", lineSep='\n')

            
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
