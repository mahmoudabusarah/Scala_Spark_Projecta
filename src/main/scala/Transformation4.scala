import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Transformation4 extends App {
  val spark = SparkSession.builder()
    .appName("ScalaSparkTransformations")
    .master("local") // Set master to "local" for local mode
    .getOrCreate()

  val csvFilePath = raw"C:/Users/Consultant/Downloads/sample_data.csv"

  // Read the CSV file
  val df: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(csvFilePath)

  // Group by age and calculate the average salary
  val resultDf = df.groupBy("age")
    .agg(avg("salary").alias("average_salary"))

  // Show the resulting DataFrame
  resultDf.show()

  // Stop the SparkSession
  spark.stop()
}
