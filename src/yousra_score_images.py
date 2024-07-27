from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log, when, datediff, current_date, row_number ,lit
from pyspark.sql.window import Window

class ImageScorer:
    def __init__(self, spark, images_df, tags_df):
        self.spark = spark
        self.images_df = images_df
        self.tags_df = tags_df
        
        # Constants
        self.MIN_RES = 160000
        self.MAX_RES = 2073600
        self.MIN_AR = 0.3
        self.MAX_AR = 4.65
        self.MAX_FRESHNESS_DAY = 10 * 365
        self.WEIGHT_RESOLUTION = 6
        self.WEIGHT_ASPECT_RATIO = 2
        self.WEIGHT_FRESHNESS = 2
        self.WEIGHT_TAG_PRIORITY = 3

    def prepare_features(self):
        self.images_df = self.images_df.withColumn("resolution", col("width") * col("height"))
        self.images_df = self.images_df.withColumn("aspect_ratio", when(col("height") == 0, None).otherwise(col("width") / col("height")))
        return self.images_df

    def calculate_scores(self):
        # Calculate score of resolution
        self.images_df = self.images_df.withColumn("score_res", when(col("resolution") <= 0, 0).otherwise(
            (log(col("resolution")) - log(lit(self.MIN_RES))) / (log(lit(self.MAX_RES)) - log(lit(self.MIN_RES)))
        ))
        self.images_df = self.images_df.withColumn("score_res", when(col("score_res") > 1, 1).otherwise(col("score_res")))

        # Calculate score of aspect ratio
        self.images_df = self.images_df.withColumn("score_ar", when(col("aspect_ratio").isNull(), 0).otherwise(
            when(col("aspect_ratio") < self.MIN_AR, 0).when(col("aspect_ratio") > self.MAX_AR, 0).otherwise(1)
        ))

        # Calculate score of freshness
        self.images_df = self.images_df.withColumn("score_fresh", 1 + (-1 * datediff(current_date(), col("created_at")) / self.MAX_FRESHNESS_DAY))
        self.images_df = self.images_df.withColumn("score_fresh", when(col("score_fresh") < 0, 0).when(col("score_fresh") > 1, 1).otherwise(col("score_fresh")))

        # Join with tags_df to get the tag priority scores
        self.images_df = self.images_df.join(self.tags_df.select("image_id", "probability"), on="image_id", how="left").fillna({"probability": 0})
        self.images_df = self.images_df.withColumn("score_tag", col("probability"))

        # Calculate the overall image score
        self.images_df = self.images_df.withColumn("score_image", (
            self.WEIGHT_RESOLUTION * col("score_res") +
            self.WEIGHT_ASPECT_RATIO * col("score_ar") +
            self.WEIGHT_FRESHNESS * col("score_fresh") +
            self.WEIGHT_TAG_PRIORITY * col("score_tag")
        ) / (self.WEIGHT_RESOLUTION + self.WEIGHT_ASPECT_RATIO + self.WEIGHT_FRESHNESS + self.WEIGHT_TAG_PRIORITY))
        
        return self.images_df

    def rank_and_select_main_image(self):
        windowSpec = Window.partitionBy("hotel_id").orderBy(col("score_image").desc())
        ranked_df = self.images_df.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") == 1)
        return ranked_df

    def execute(self):
        self.prepare_features()
        self.calculate_scores()
        main_images_df = self.rank_and_select_main_image()
        return main_images_df

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Image Scoring").getOrCreate()
    
    # Sample DataFrames
    images_df = spark.createDataFrame([
        (1, 100, 200, "2023-07-01", "tag1", 0.9),
        (1, 100, 200, "2023-07-01", "tag2", 0.8),
        (2, 150, 300, "2023-01-01", "tag1", 0.7),
        (3, 200, 400, "2022-07-01", "tag3", 0.6)
    ], ["image_id", "height", "width", "created_date", "tag_id", "probability"])

    tags_df = spark.createDataFrame([
        ("tag1", 1),
        ("tag2", 2),
        ("tag3", 3)
    ], ["tag_id", "priority_score"])

    # Create an instance of ImageScorer and execute the process
    image_scorer = ImageScorer(spark, images_df, tags_df)
    main_images_df = image_scorer.execute()

    # Show the result
    main_images_df.show()