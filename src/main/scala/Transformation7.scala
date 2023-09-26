import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Transformation7  extends App{
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

  // Show the original DataFrame
  df.show()

  // Drop the "salary" column
  val dfWithoutSalary = df.drop("salary")

  // Show the DataFrame after dropping the "salary" column
  dfWithoutSalary.show()

  // Stop the SparkSession when done
  spark.stop()

}
