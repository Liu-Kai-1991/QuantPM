package qpm.system.configuration

import java.io.File
import java.nio.file.{Files, Paths}

import com.typesafe.config.{Config, ConfigFactory}
import qpm.system.Log

trait Configuration extends Log{
  protected case class Init private()

  val configurationFolder = "config"
  def configurationFile: String
  protected def initialized: Init

  protected def config: Config = ConfigFactory.parseFile(new File(configPath))
  protected def configPath: String = s"$configurationFolder/$configurationFile"
  protected def init(): Init = {
    if (!Files.exists(Paths.get(configurationFolder))){
      log.warn(s"Configuration folder not exist, creating new folder $configurationFolder")
      new File(configurationFolder).mkdir()
    }
    if (!Files.exists(Paths.get(configPath))){
      log.warn(s"Configuration file not exist, creating new file $configPath")
      new File(configPath).createNewFile()
    }
    Init()
  }
}
