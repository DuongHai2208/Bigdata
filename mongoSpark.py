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


    sqlDF = spark.sql("SELECT * FROM products WHERE ProductPrice < 1000")
    sqlDF.show()


    spark.stop()
