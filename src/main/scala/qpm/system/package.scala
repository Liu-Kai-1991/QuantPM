package qpm
import java.time.{LocalDate, ZoneId}
import java.util.Date


package object system {
  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]):Traversable[(X,Y)] = for { x <- xs; y <- ys } yield (x, y)
  }

  implicit class LocalDate2DateImplicit(val date: LocalDate) extends AnyVal{
    def asDate: Date = Date.from(date.atStartOfDay(ZoneId.systemDefault).toInstant)
  }
}
