package samples.utils

import org.apache.spark
import org.apache.spark.sql.types._
import samples.config.Field

//Objet qui relie le fichier json à notre fonction buildDataframe

object DataFrameSchema {

  def buildDataframeSchema(fields: Seq[Field]): StructType = {
    val structFieldsList = fields.map(field => {
      spark.sql.types.StructField(field.name, mapPrimitiveTypesToSparkTypes(field.`type`))
    })

    spark.sql.types.StructType(structFieldsList)
  }

  //On associe les types au format attendu en scala
  def mapPrimitiveTypesToSparkTypes(`type`:String): DataType = {
    `type` match {
      case "String" => StringType
      case "Double" => DoubleType
      case "Long" => LongType
      case _ => StringType
    }
  }

}
