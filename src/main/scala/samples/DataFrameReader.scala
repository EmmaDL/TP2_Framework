package samples

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import samples.config.CsvOptions

//Objet Scala qui fait applique notre schéma de dataframe aux données ciblées
object DataFrameReader {
  def readCsv(path: String, options: CsvOptions, schema: StructType)(implicit sparkSession: SparkSession): DataFrame = {

    sparkSession.read
      .option("delimiter", options.delimiter)
      .option("header", options.header)
      .option("inferSchema", options.inferSchema)
      .schema(schema)
      .csv(path)
  }
}
