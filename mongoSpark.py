from pyspark.sql import SparkSession
from pyspark.sql import Row

def parseInput(line):
    fields = line.split(',')
    return Row(
        ProductID = int(fields[0]),
        ProductCategory = fields[1],
        ProductBrand = fields[2],
        ProductPrice = float(fields[3]),
        CustomerAge = int(fields[4]),
        CustomerGender = int(fields[5]),  # 0: Female, 1: Male
        PurchaseFrequency = int(fields[6]),
        CustomerSatisfaction = int(fields[7]),
        PurchaseIntent = int(fields[8])
    )

if __name__ == "__main__":

    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    df = spark.read.option("header", "true").csv("hdfs:///user/maria_dev/mongodb/consumer_electronics_sales_data.csv")
    lines = df.rdd.map(lambda row: ",".join(row))

    products = lines.map(parseInput)
    

    productsDataset = spark.createDataFrame(products)


    productsDataset.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/moviesdata.products")\
        .mode('append')\
        .save()


    readProducts = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/moviesdata.products")\
        .load()


    readProducts.createOrReplaceTempView("products")


    print("Products with price below 1000:")
    low_price_products = spark.sql("SELECT ProductID, ProductCategory, ProductPrice FROM products WHERE ProductPrice < 1000")
    low_price_products.show()

    # 2. Grouping and Aggregation: Calculate average price per category
    print("Average price per product category:")
    avg_price_per_category = spark.sql("""
        SELECT ProductCategory, AVG(ProductPrice) AS avg_price
        FROM products
        GROUP BY ProductCategory
        ORDER BY avg_price DESC
    """)
    avg_price_per_category.show()

    # 3. Grouping by multiple columns: Find the total purchase frequency per category and brand
    print("Total purchase frequency per category and brand:")
    total_purchase_frequency = spark.sql("""
        SELECT ProductCategory, ProductBrand, SUM(PurchaseFrequency) AS total_purchase_frequency
        FROM products
        GROUP BY ProductCategory, ProductBrand
        ORDER BY total_purchase_frequency DESC
    """)
    total_purchase_frequency.show()

    # 4. Complex Filtering: Find products with high customer satisfaction and high purchase intent
    print("High satisfaction and high purchase intent products:")
    high_satisfaction = spark.sql("""
        SELECT ProductID, ProductCategory, ProductBrand, ProductPrice, CustomerSatisfaction, PurchaseIntent
        FROM products
        WHERE CustomerSatisfaction > 4 AND PurchaseIntent > 2
        ORDER BY CustomerSatisfaction DESC, PurchaseIntent DESC
    """)
    high_satisfaction.show()

    # 5. Statistical summary (e.g., Price, Age, Frequency)
    print("Statistical summary of Product Price, Customer Age, and Purchase Frequency:")
    stats = readProducts.select("ProductPrice", "CustomerAge", "PurchaseFrequency")
    stats.describe().show()

    # 6. Windowing and Ranking: Ranking products by Customer Satisfaction
    from pyspark.sql.window import Window
    print("Ranking products by Customer Satisfaction:")
    windowSpec = Window.orderBy(col("CustomerSatisfaction").desc())
    ranked_products = readProducts.withColumn("rank", rank().over(windowSpec))
    ranked_products.select("ProductID", "ProductCategory", "ProductBrand", "CustomerSatisfaction", "rank").show()



    spark.stop()
