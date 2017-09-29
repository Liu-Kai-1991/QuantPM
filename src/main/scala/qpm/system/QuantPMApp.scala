package qpm.system

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.kohsuke.args4j.spi.{OneArgumentOptionHandler, Setter}
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, OptionDef}

import scala.collection.JavaConverters._

trait QuantPMCmdLine {
  type LocalDateHandler = QuantPMCmdLine.LocalDateHandler
  type LocalDateOptionHandler = QuantPMCmdLine.LocalDateOptionHandler
}

object QuantPMCmdLine {

  object LocalDateHandler {
    private[QuantPMCmdLine] val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  }

  class LocalDateHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[LocalDate])
    extends OneArgumentOptionHandler[LocalDate](parser, option, setter) {
    override def parse(argument: String): LocalDate = LocalDate.parse(argument, LocalDateHandler.dateTimeFormatter)
  }

  class LocalDateOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[LocalDate]])
    extends OneArgumentOptionHandler[Option[LocalDate]](parser, option, setter) {
    override def parse(argument: String): Option[LocalDate] =
      Some(LocalDate.parse(argument, LocalDateHandler.dateTimeFormatter))
  }

}

class QuantPMApp[CmdLine <: QuantPMCmdLine](cmd: CmdLine) extends App {
  val cmdLine = cmd
  val parser = new CmdLineParser(cmd)
  try {
    parser.parseArgument(args.toList.asJava)
  } catch {
    case e: CmdLineException =>
      print(s"Error:${e.getMessage}\n Usage:\n")
      parser.printUsage(System.out)
      System.exit(1)
  }
}