import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Transformation2  extends App {
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

  // Select specific columns (name and age) and filter rows where age > 30
  val selectedAndFiltered = df.select("name", "age").filter(col("age") > 30)

  // Show the DataFrame with selected columns and filtered rows
  selectedAndFiltered.show()
}
