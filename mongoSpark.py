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
        .option("uri", "mongodb://127.0.0.1/customerData.products")\
        .mode('append')\
        .save()


    readProducts = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/customerData.products")\
        .load()


    readProducts.createOrReplaceTempView("products")

    print("Total purchase frequency per category and brand:")
    total_purchase_frequency = spark.sql("""
        SELECT ProductCategory, ProductBrand, SUM(PurchaseFrequency) AS total_purchase_frequency
        FROM products
        GROUP BY ProductCategory, ProductBrand
        ORDER BY total_purchase_frequency DESC
    """)
    total_purchase_frequency.show()
    
    from pyspark.sql.window import Window
    print("Ranking products by Customer Satisfaction:")
    windowSpec = Window.orderBy(col("CustomerSatisfaction").desc())
    ranked_products = readProducts.withColumn("rank", rank().over(windowSpec))
    ranked_products.select("ProductID", "ProductCategory", "ProductBrand", "CustomerSatisfaction", "rank").show()

    high_satisfaction.show()



    spark.stop()
