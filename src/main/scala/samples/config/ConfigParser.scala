package samples.config
import samples.config.ConfigParser.builder.{opt, programName}
import scopt.{DefaultOParserSetup, OParser, OParserBuilder, OParserSetup}

case class ConfigParser(inputPath: String = "", inputFormat: String = "",
                        outputPath: String = "", outputFormat: String ="",
                        partitions: String = "", id: String = "", delimiter: String = "")

object ConfigParser {
  private val setup: OParserSetup = new DefaultOParserSetup {
    override def showUsageOnError: Option[Boolean] = Some(true)
    override def errorOnUnknownArgument = false
  }
  val builder: OParserBuilder[ConfigParser] = OParser.builder[ConfigParser]

  val parser: OParser[Unit, ConfigParser] = {

    OParser.sequence(
      programName("GDPR Compliance"),

      opt[String]("inputPath")
        .required()
        .action((x, c) => c.copy(inputPath = x))
        .text("input path of my program"),

      opt[String]("inputFormat")
        .required()
        .action((x, c) => c.copy(inputFormat = x))
        .text(""),

      opt[String]("outputPath")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("output path of my program"),

      opt[String]("outputFormat")
        .required()
        .action((x, c) => c.copy(outputFormat = x))
        .text(""),

      opt[String]("partitions")
        .required()
        .action((x, c) => c.copy(partitions = x))
        .text(""),

      opt[String]("id")
      .required()
      .action((x, c) => c.copy(id = x))
      .text(""),

      opt[String]("delimiter")
        .required()
        .action((x, c) => c.copy(delimiter = x))
        .text("")
    )
  }

  def parser(arguments: Array[String]): Option[ConfigParser] = {
    OParser.parse(ConfigParser.parser, arguments, ConfigParser(), setup)
  }

  def getConfigArgs(args: Array[String]): ConfigParser = {
    ConfigParser.parser(args) match {
      case Some(config) => config
      case _ => {
        print("cannot parse conf")
        sys.exit(1)
      }
    }
  }
}
