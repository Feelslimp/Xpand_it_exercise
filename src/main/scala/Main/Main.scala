package Main

import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import jobs.processdata
import org.apache.spark.sql.functions.{avg, col, count, explode}
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Main")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
      .getOrCreate()
    import spark.implicits._
    val filePath_reviews = "src/main/resources/googleplaystore_user_reviews.csv"
    val filePath = "src/main/resources/googleplaystore.csv"

    //Part 1 crating the df_1 dataframe
    val df_1 = jobs.processdata.processData_df1(spark, filePath_reviews)
    //df_1.show()

    //Part 2
    val df_2 = jobs.processdata.processData_df2(spark, filePath)
    //df_2.show
    val outputPath1 = "results/best_apps.csv"
    //to avoid error about writing some nulls theres this fill option
    //val cleanedDF = df_2.na.fill("N/A")
    jobs.writedata.writeCSV(df_2, outputPath1,'§')

    //part 3
    val df_3 = jobs.processdata.processData_df3(spark,filePath)
    //df_3.show()

    //Part 4
    // just a join to add the Average_sentiment_polarity
    val df_joined = df_3.join(df_1.select("App", "Average_Sentiment_Polarity"), Seq("App"), "left")
    //df_joined.show()
    val outputPath2 = "results/googleplaystore_cleaned"
    jobs.writedata.writeParquetGzip(df_joined,outputPath2)

    //Part 5
    val df_explode = df_joined.withColumn("Genre", explode(col("Genres")))
    // I calculate the count seperatly because there is a lot of ratings that are null and it breaks the avg functio-n
    val df_count = df_explode.groupBy("Genre").agg(count("App").alias("Count"))
    // here i drop the the empty cells so i can calculate the averages
    val df_avg = df_explode
      .na.drop(Seq("Rating", "Average_Sentiment_Polarity"))
      .groupBy("Genre")
      .agg(avg("Rating").alias("Average_Rating"),
           avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
    // join of both dataframes
    val df_4 = df_count.join(df_avg, Seq("Genre"), "left")
    //df_4.show()
    val outputPath3 = "results/googleplaystore_metrics"
    jobs.writedata.writeParquetGzip(df_4,outputPath3)


    spark.stop
  }

}
