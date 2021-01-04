package samples.config

import scala.io.Source

//Objet qui permet de lire le fichier json
object ConfigReader {
  def readConfig(configPath: String): String = {
    Source.fromFile(configPath).getLines().mkString
  }
}

