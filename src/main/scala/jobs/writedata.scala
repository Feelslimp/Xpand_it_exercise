package jobs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.commons.csv.CSVFormat
import java.io.FileWriter
object writedata {
  /*this is the simple method but its causing some hadoop problems, so i will use an alternative just to make sure it works
  def writeCSV(df: DataFrame, outputPath: String, delimiter: String = "§"): Unit = {
    df.write
      .option("header", "true")
      .option("delimiter", delimiter)
      .mode("overwrite")
      .csv(outputPath)
  }
   */
  def writeCSV(df: DataFrame, outputPath: String, delimiter: Char = '§'): Unit = {
    val writer = new FileWriter(outputPath)
    try {
      val csvPrinter = CSVFormat.DEFAULT
        .builder()
        .setDelimiter(delimiter)
        .build()
        .print(writer)

      df.collect().foreach { row =>
        val values = row.toSeq.map {
          case null => ""
          case value => value.toString
        }
        csvPrinter.printRecord(values: _*)
      }
      csvPrinter.flush()
    } finally {
      writer.close()
    }
  }

  //just a writer of parquet with the gzip compression
  def writeParquetGzip(df: DataFrame, outputPath: String): Unit = {
    df.write
      .option("compression", "gzip")
      .parquet(outputPath)
  }
}