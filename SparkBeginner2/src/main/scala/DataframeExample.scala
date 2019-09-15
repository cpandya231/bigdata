
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}


object DataframeExample {



  def main(args: Array[String]): Unit = {


  val spark = SparkSession.builder().master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    import spark.implicits._
    val column= Seq(("hii",2,1l)).toDF("col1","col2","col3")
    column.show(10)

    val retailData=spark.read.format("csv").option("header","true").option("inferSchema","true")
      .load("/home/chintan/Downloads/spark-2.4.0-bin-hadoop2.7/data/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

    retailData.show(10)
  }
}
