package qpm.data.general

import java.time.{Instant, Period, ZoneId, ZonedDateTime}
import java.util.Date

object DateUtil {
  implicit class InstantImplicit(val instant: Instant) extends AnyVal {
    def toEod(zoneId: ZoneId): Instant = {
      val zdt = ZonedDateTime.ofInstant(instant, zoneId)
      val zdtStart = zdt.toLocalDate.atStartOfDay(zoneId)
      val zdtNextStart = zdtStart.plusDays(1)
      val zdtEnd = zdtNextStart.minusNanos(1)
      zdtEnd.toInstant
    }

    def toSod(zoneId: ZoneId): Instant = {
      val zdt = ZonedDateTime.ofInstant(instant, zoneId)
      zdt.toLocalDate.atStartOfDay(zoneId).toInstant
    }

    def toEodDate(zoneId: ZoneId): Date =
      Date.from(toEod(zoneId))

    def toSodDate(zoneId: ZoneId): Date =
      Date.from(toSod(zoneId))

    def minusDays(days: Int): Instant =
      instant.minus(Period.ofDays(days))
  }
}
