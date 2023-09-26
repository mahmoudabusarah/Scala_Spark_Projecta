import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Transformation6 extends App {
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

  // Add a new column "is_above_30" with the conditional expression
  val dfWithNewColumn = df.withColumn("is_above_30", when(col("age") > 30, "Yes").otherwise("No"))

  // Show the DataFrame with the new column
  dfWithNewColumn.show()
}
