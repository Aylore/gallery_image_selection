from src.join_chuncks import join_with_directory
from utils.create_chunks import chunk_json_file
from pyspark.sql import SparkSession
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(spark, images_dir, image_tags_dir, join_col_name="image_id", split_to_chunks=None, num_of_chunks=None):
    """
    Main function to join image and tag datasets, with optional chunking for large datasets.

    :param spark: SparkSession object.
    :param images_dir: Path to the images JSONL file.
    :param image_tags_dir: Path to the image tags JSONL file.
    :param join_col_name: Column name to join on.
    :param split_to_chunks: Specifies which files to split into chunks ('images', 'tags', 'both', or None).
    :param num_of_chunks: Number of chunks to split the files into.
    """
    try:
        if  num_of_chunks:
            raise ValueError("")

        # Read images and tags JSONL files
        df_imgs = spark.read.json(images_dir)
        df_tags = spark.read.json(image_tags_dir)

        # Join images and tags dataframes
        logging.info(f"Joining images and tags on column: {join_col_name}")
        df_imgs_tags = df_imgs.join(df_tags, [join_col_name], "left")
        logging.info(f"Joined dataframe count: {df_imgs_tags.count()}")

    except Exception as ex:
        if not num_of_chunks:
            logging.error(f"Could not join images with tags due to: {ex}")
        logging.info("Attempting to split the files into chunks")

        # Split JSONL files into chunks
        if split_to_chunks:
            if split_to_chunks in ["images", "tags", "both"]:
                paths_to_split = [images_dir] if split_to_chunks == "images" else [image_tags_dir]
                if split_to_chunks == "both":
                    paths_to_split = [images_dir, image_tags_dir]

                output_dirs = [path.replace(".jsonl", "_chunks/") for path in paths_to_split]

                for input_path, output_dir in zip(paths_to_split, output_dirs):
                    logging.info(f"Splitting {input_path} into chunks")
                    chunk_json_file(input_file_path=input_path, output_directory=output_dir, num_chunks=num_of_chunks)
            else:
                logging.warning(f"Invalid option for split_to_chunks: {split_to_chunks}")
        else:
            logging.warning("No split_to_chunks option provided, skipping chunking")

if __name__ == "__main__":
    setup_logging()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("JoinWithDirectory") \
        .getOrCreate()

    # Parameters
    images_dir = 'data/images.jsonl'
    image_tags_dir = 'data/image_tags.jsonl'
    join_col_name = "image_id"
    split_to_chunks = "both"
    num_of_chunks = 3

    # Example usage
    main(spark, images_dir, image_tags_dir, join_col_name, split_to_chunks, num_of_chunks)
