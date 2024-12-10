from pyspark.sql import SparkSession
from pyspark.sql import Row

# Hàm để phân tích dữ liệu từ dòng văn bản
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
    # Tạo SparkSession
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    # Đọc dữ liệu từ HDFS hoặc tệp văn bản
    lines = spark.sparkContext.textFile("hdfs:///path/to/your/data.csv")  # Đường dẫn tới tệp của bạn
    
    # Tạo RDD từ dữ liệu và áp dụng hàm parseInput
    products = lines.map(parseInput)
    
    # Chuyển đổi RDD thành DataFrame
    productsDataset = spark.createDataFrame(products)

    # Ghi dữ liệu vào MongoDB
    productsDataset.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/moviesdata.products")\
        .mode('append')\
        .save()

    # Đọc lại dữ liệu từ MongoDB
    readProducts = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/moviesdata.products")\
        .load()

    # Tạo một bảng tạm trong Spark SQL
    readProducts.createOrReplaceTempView("products")

    # Thực hiện truy vấn SQL để chọn các sản phẩm có giá dưới 1000
    sqlDF = spark.sql("SELECT * FROM products WHERE ProductPrice < 1000")
    sqlDF.show()

    # Dừng SparkSession
    spark.stop()
