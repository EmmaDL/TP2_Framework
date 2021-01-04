package samples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import samples.config.JsonConfigProtocol.jsonConfig
import samples.config.{ConfigReader, JsonConfig}
import samples.utils.DataFrameSchema
import spray.json._


object SampleProgram {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()


    val confReader = ConfigReader.readConfig("conf/config.json")
    val configuration = confReader.parseJson.convertTo[JsonConfig]

    val dfSchema: StructType = DataFrameSchema.buildDataframeSchema(configuration.fields)
    val data = DataFrameReader.readCsv("input_data\\*.csv", configuration.csvOptions, dfSchema)

    //    data.printSchema()
    //    data.show(40,false)

    spark.udf.register("get_file_name", (path: String) => path.split("/").last.split("\\.").head)
    val question2 = data.withColumn("date", callUDF("get_file_name", input_file_name()))
//    question2.show(40,false)


//    import org.apache.spark.sql.expressions.Window
//    val window = Window.orderBy("date")
//    val question3 = question2.filter(col("currency") === "DOL")
//    val lagCol = lag(col("amount"), 1).over(window)
//    question3.withColumn("amount", lagCol).show()


    import java.time.format.DateTimeFormatter

    val to_date = udf((date: String, numberFormat: String, destFormat: String) => {
      Option(date).map(number => {
        val date = number.toString
        val formatFrom = DateTimeFormatter.ofPattern(numberFormat)
        val formatTo = DateTimeFormatter.ofPattern(destFormat)
        formatTo.format(formatFrom.parse(date))
      })
    })


    val destFormat = "yyyy-MM-dd"
    val convertedDates = question2.select(to_date(col("date"), lit("ddMMyyyy"), lit(destFormat)))

    val df1 = question2.withColumn("id", monotonically_increasing_id())
    val df2 = convertedDates.withColumn("id", monotonically_increasing_id())
    val df3 = df2.join(right = df1,usingColumn = "id").drop("id").drop("date")

    val renamed_df: DataFrame = df3.withColumnRenamed("UDF(date, ddMMyyyy, yyyy-MM-dd)", "date").orderBy("date")

//    renamed_df.show(40,false)

    val question3_2 = renamed_df.withColumn("new_date",array(col("date"),date_add(col("date"),+1))).
      drop("date").
      selectExpr("explode(new_date) as date","*").
      drop("new_date").
      dropDuplicates("date","currency").orderBy("date")

    question3_2.show(60,false)

//    Question4
    question3_2.write.mode(SaveMode.Overwrite).partitionBy("date").csv("output_data")

  }



}
