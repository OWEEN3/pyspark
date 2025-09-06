from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class ProductCategoryProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_product_category_pairs(self, products_df, categories_df, product_categories_df):
        joined = products_df.join(product_categories_df, "product_id", "left") \
            .join(categories_df, "category_id", "left")

        result = joined.select(
            col("product_name"),
            col("category_name")
        ).orderBy("product_name")

        return result

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("ProductCategoryApp").getOrCreate()

    products = spark.createDataFrame([
        (1, "Product A"),
        (2, "Product B"),
        (3, "Product C")
    ], ["product_id", "product_name"])

    categories = spark.createDataFrame([
        (1, "Category 1"),
        (2, "Category 2")
    ], ["category_id", "category_name"])

    product_categories = spark.createDataFrame([
        (1, 1),
        (1, 2),
        (2, 1)
    ], ["product_id", "category_id"])

    processor = ProductCategoryProcessor(spark)
    result_df = processor.get_product_category_pairs(products, categories, product_categories)
    
    result_df.show(truncate=False)
