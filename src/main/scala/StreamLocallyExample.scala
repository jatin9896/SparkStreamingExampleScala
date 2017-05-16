import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object StreamLocallyExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("StreamLocallyExample")
      .config("spark.master", "local")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
