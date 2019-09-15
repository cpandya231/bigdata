
import org.apache.spark.sql.SparkSession

object Demo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local")
      .getOrCreate()
    val flightData = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true").load("src/main/resources/2015-summary.csv")
    flightData.createOrReplaceTempView("flight_data_2015")

    flightData.sample(false, 0.5, 5).show(10)


    val schema = flightData.schema

    val demoRdd = spark.sparkContext.parallelize(List("Chintan", "Big Data", "Learning"))


  }
}
