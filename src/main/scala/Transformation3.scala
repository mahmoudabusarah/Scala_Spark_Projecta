import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Transformation3 extends App {
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

  // Add a new column "NewSalary" that is the sum of "Salary" and 10,000
  val dfWithNewSalary = df.withColumn("NewSalary", col("Salary") + 10000)

  // Show the DataFrame with the new column
  dfWithNewSalary.show()
}
