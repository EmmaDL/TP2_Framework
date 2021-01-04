package samples.config

import spray.json.{DefaultJsonProtocol, JsonFormat}

//Object qui permet de parser notre fichier config.json
case class JsonConfig(name:String, csvOptions: CsvOptions, fields: Seq[Field])
case class CsvOptions(header:String, inferSchema: String, delimiter: String)
case class Field(name:String, `type`: String)

object JsonConfigProtocol extends DefaultJsonProtocol {
  implicit val csvOptions: JsonFormat[CsvOptions] = lazyFormat(jsonFormat3(CsvOptions))
  implicit val field: JsonFormat[Field] = lazyFormat(jsonFormat2(Field))
  implicit val jsonConfig: JsonFormat[JsonConfig] = lazyFormat(jsonFormat3(JsonConfig))

}
