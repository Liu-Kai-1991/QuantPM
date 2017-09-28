package qpm.system

import java.util.Calendar
import java.text.SimpleDateFormat
import scala.io.AnsiColor
import Log.LogType
import java.io.FileOutputStream

case class Logger(c: Class[_]){
  def writeLog(logType: LogType, message: String): Unit =
    Log.writeLog(logType, c.getName, message)
  def debug(message: String): Unit = writeLog(Log.DEBUG, message)
  def info(message: String): Unit = writeLog(Log.INFO, message)
  def warn(message: String): Unit = writeLog(Log.WARN, message)
}

trait Log {
  val log = Logger(this.getClass)
}


object Log extends Enumeration {
  val INFO, WARN, DEBUG = Value
  type LogType = Value

  private var logLevel: LogType = INFO

  def setLogLevel(logType: LogType): Unit = {
    logLevel = logType
  }

  private val dateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val logFileName = s"target/archive/${new SimpleDateFormat("yyyyMMdd-HHmmss").
    format(Calendar.getInstance.getTime)}.log"

  private object LogFileLocker

  private val logOutputStream = new FileOutputStream(logFileName)

  def writeLog(logType: LogType, classRef: String, message: String): Unit = {
    if (logType == DEBUG && logLevel!=DEBUG) Unit else
    if (logType == INFO && logLevel==DEBUG) Unit else {
      val backgroundColor = logType match {
        case INFO => AnsiColor.BOLD
        case WARN => AnsiColor.YELLOW_B
        case DEBUG => ""
      }

      val timeStamp = dateFormat.format(Calendar.getInstance.getTime)

    val logMessage =
      s"$timeStamp - $classRef - [$logType] ${AnsiColor.RESET}$backgroundColor $message ${AnsiColor.RESET}"

      println(logMessage)
      LogFileLocker.synchronized{
        logOutputStream.write(s"$timeStamp - $classRef - [$logType] $message\n".getBytes)
        logOutputStream.flush()
      }
    }
  }
}

