

import org.apache.spark.sql.SparkSession
object ReadingFromDBExample {



  def main(args: Array[String]): Unit = {


  val spark = SparkSession.builder().master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val driver = "org.sqlite.JDBC"
    val path = "/home/chintan/Downloads/spark-2.4.0-bin-hadoop2.7/data/Spark-The-Definitive-Guide/data/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:/${path}"
    val tablename = "flight_info"

    //To test connection
//    val connection= DriverManager.getConnection(url);
//    connection.isClosed()

    val dbDataFrame= spark.read.format("jdbc").option("url",url).option("dbtable",tablename).option("driver",driver).load()

    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)

    // Reading data by partitioning predicates

    val props = new java.util.Properties
    props.setProperty("driver","org.sqlite.JDBC")

    val predicates = Array(
      "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
      "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")

    spark.read.jdbc(url,tablename,predicates,props).show()
    println(spark.read.jdbc(url, tablename, predicates, props).rdd.getNumPartitions)
  }
}
