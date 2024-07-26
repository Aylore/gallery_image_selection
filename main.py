import logging
from pyspark.sql import SparkSession
from utils.logging_config import setup_logging
from utils.validate_json_schema import validate_jsonl
from utils.read_pyspark import JSONDataProcessor

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

class DataJoiner:
    """Class to join DataFrames."""
    @staticmethod
    def join_data(df_imgs, df_tags, join_col_name):
        logger.info(f"Joining images and tags on column: {join_col_name}")
        df_imgs_tags = df_imgs.join(df_tags, [join_col_name], "left")
        logger.info(f"Joined dataframe count: {df_imgs_tags.count()}")
        return df_imgs_tags

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
            df_imgs = loader.load_images(self.images_dir)
            df_tags = loader.load_image_tags(self.image_tags_dir)
            df_main_imgs = loader.load_main_images(self.main_images_dir)

            # Join data
            df_imgs_tags = DataJoiner.join_data(df_imgs, df_tags, self.join_col_name)

            # Additional processing can be added here

        except Exception as ex:
            logger.error(f"An error occurred: {ex}")
            raise

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("JoinWithDirectory") \
        .getOrCreate()

    # Define paths and schemas
    images_dir = 'data/images.jsonl'
    image_tags_dir = 'data/image_tags.jsonl'
    main_images_dir = 'data/main_images.jsonl'
    images_dir_schema = 'schemas/image.json'
    image_tags_dir_schema = 'schemas/image_tags.json'
    main_images_dir_schema = 'schemas/main_image.json'
    join_col_name = "image_id"

    # Create the processor and run the main processing function
    processor = ImageTagProcessor(spark, images_dir, image_tags_dir, main_images_dir, images_dir_schema, image_tags_dir_schema, main_images_dir_schema, join_col_name)
    processor.process()

    # Stop Spark session
    spark.stop()
