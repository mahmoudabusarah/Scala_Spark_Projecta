import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


object Transformation5 extends App {
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

  // Sort the DataFrame by the "age" column in ascending order
  val sortedDF = df.orderBy("age")

  // Show the sorted DataFrame
  sortedDF.show()
}
