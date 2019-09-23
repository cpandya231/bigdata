

import org.apache.spark.sql.SparkSession

object ReadingFromMySqlDBExample {



  def main(args: Array[String]): Unit = {


  val spark = SparkSession.builder().master("local")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")


    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/mydb"
    val tablename = "employee"
    val user="admin"
    val password="password"


    val activityQuery= spark.read.format("jdbc").option("url",url).option("dbtable",tablename).option("driver",driver).
      option("user",user).option("password",password).load()
    activityQuery.show()

  }
}
