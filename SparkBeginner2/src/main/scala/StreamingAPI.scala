
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}


object StreamingAPI {

  case class FLIGHT(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val retailData = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("/home/chintan/Downloads/spark-2.4.0-bin-hadoop2.7/data/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

    retailData.createOrReplaceTempView("retail_data")

    val retailSchema = retailData.schema

    val retailDataStream = spark.readStream.
      schema(retailSchema)
      .option("maxFilesPerTrigger", "1")
      .format("csv")
      .option("header", "true")
      .load("/home/chintan/Downloads/spark-2.4.0-bin-hadoop2.7/data/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

    retailData.createOrReplaceTempView("retail_data")


    val purchaseByCustomerPerHour =
      retailDataStream.selectExpr("CustomerID", "(UnitPrice*Quantity) as total_cost", "InvoiceDate").groupBy(col("CustomerID")
        , window(col("InvoiceDate"), "1 day")).sum("total_cost")

    purchaseByCustomerPerHour.writeStream.format("console").queryName("customer_purchases").outputMode("complete").start()


  }
}
