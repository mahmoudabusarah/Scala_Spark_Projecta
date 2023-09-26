import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Transformation9 extends App {
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

  // Create a new DataFrame (df2)
  val df2 = spark.createDataFrame(Seq(("Mike", 40, 80000))).toDF("Name", "Age", "Salary")

  // Union df and df2
  val unionDF = df.union(df2)

  // Show the resulting DataFrame after the union
  unionDF.show()
}
