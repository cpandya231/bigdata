

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

object DataframeExample {



  def main(args: Array[String]): Unit = {


  val spark = SparkSession.builder().master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")


    val myManualSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))
    //Reading from a CSV file
    val retailData=spark.read.format("csv").option("header","true").option("inferSchema","true")
      .load("/home/chintan/Downloads/spark-2.4.0-bin-hadoop2.7/data/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")

    retailData.show(10)



    //Reading from a json file
    val retailDataJson=spark.read.format("json").option("mode","FAILFAST").schema(myManualSchema)
      .load("/home/chintan/Downloads/spark-2.4.0-bin-hadoop2.7/data/Spark-The-Definitive-Guide/data/flight-data/json/2010-summary.json")
    retailDataJson.show(10)

    //To write csv data in json
    retailData.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")

    //Parquet file
    val retailDataParquet=spark.read.format("parquet")
      .load("/home/chintan/Downloads/spark-2.4.0-bin-hadoop2.7/data/Spark-The-Definitive-Guide/data/flight-data/parquet/2010-summary.parquet")
    retailDataParquet.show(10)

    //Orc file
    val retailDataOrc=spark.read.format("orc")
      .load("/home/chintan/Downloads/spark-2.4.0-bin-hadoop2.7/data/Spark-The-Definitive-Guide/data/flight-data/orc/2010-summary.orc")
    retailDataOrc.show(10)
  }
}
