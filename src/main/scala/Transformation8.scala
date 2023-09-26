import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Transformation8 extends App {
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

  // Rename the "name" column to "full name"
  val renamedDF = df.withColumnRenamed("name", "full name")

  // Show the DataFrame with renamed column
  renamedDF.show()
}
