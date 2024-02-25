package jobs

import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, col, collect_set, first, regexp_replace, when}
object processdata {
  def processData_df1(spark: SparkSession, filePath: String): DataFrame = {
    // read the file into a df
    val df = spark.read.option("header", "true").csv(filePath)

    // replace "nan" values with 0 in the Sentiment_Polarity column to make sure it doesnt break the average, it probably is going push the values lower.
    val cleanedDF = df.withColumn("Sentiment_Polarity", when(col("Sentiment_Polarity") === "nan", 0).otherwise(col("Sentiment_Polarity")))

    // doing the transformat5ions
    val transformedDF = cleanedDF
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    transformedDF
  }

  def processData_df2(spark: SparkSession, filePath: String): DataFrame = {
    // read the file into a df
    val df = spark.read.option("header", "true").csv(filePath)
    // replace "nan" values with 0 in the Rating column because there are some nan and i think it breaks it.
    val cleanedDF = df.withColumn("Rating", when(col("Rating") === "NaN", 0).otherwise(col("Rating")))
    val filteredDF = cleanedDF.filter(cleanedDF("Rating").cast("float") >= 4.0)

    val sortedDF = filteredDF.sort(col("Rating").desc)

    sortedDF
  }
  //should come up with better name but its fine
  def processData_df3(spark: SparkSession, filePath: String): DataFrame = {
    // read the file into a df
    val df = spark.read.option("header", "true").csv(filePath)

    // tranformations on price,size and genres
    val dfWithSizeTransformed = df.withColumn("Size", when(col("Size").endsWith("M"), regexp_replace(col("Size"), "[^0-9.]", "").cast("double"))
        .when(col("Size").endsWith("K"), regexp_replace(col("Size"), "[^0-9.]", "").cast("double") * 0.0001)
        .otherwise(null))
      .withColumn("Price", when(col("Price").startsWith("$"), regexp_replace(col("Price"), "\\$", "").cast("double") * 0.9)
        .otherwise(null))
      .withColumn("Genres", functions.split(col("Genres"), ";"))

    // making the app unique by groping by it and then the transforming categories and renaming the necessary columns
    val df_transformed = dfWithSizeTransformed.groupBy("App").agg(
                                                  collect_set("Category").as("Categories"),
                                                  functions.max("Rating").as("Rating"),
                                                  first("Reviews").as("Reviews"),
                                                  first("Size").as("Size"),
                                                  first("Installs").as("Installs"),
                                                  first("Type").as("Type"),
                                                  first("Price").as("Price"),
                                                  first("Content Rating").as("Content_Rating"),
                                                  first("Genres").as("Genres"),
                                                  first("Last Updated").as("Last_Updated"),//I Know
                                                  first("Current Ver").as("Current_Ver"),
                                                  first("Android Ver").as("Minimum_Android_Version"))
    df_transformed.show()
    //3 lines on the full file get scrambled, couldnt find the reason
    df_transformed
  }
}
