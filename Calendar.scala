package org.scala.mcit

case class Calendar(
                     service_id: String,
                     monday: Int,
                     tuesday: Int,
                     wednesday: Int,
                     thursday: Int,
                     friday: Int,
                     saturday: Int,
                     sunday: Int,
                     start_date: String,
                     end_date: String
                   )

object Calendar {
  def apply(inLine: String): Calendar = {
    val p = inLine.split(",", -1)
    new Calendar(p(0), p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt, p(5).toInt, p(6).toInt, p(7).toInt, p(8), p(9))
  }
  def toCsv(calendar: Calendar): String = {
    calendar.service_id + "," +
      calendar.monday + "," +
      calendar.tuesday + "," +
      calendar.wednesday + ","+
      calendar.thursday + "," +
      calendar.friday + "," +
      calendar.saturday + ","+
      calendar.sunday + "," +
      calendar.start_date + "," +
      calendar.end_date
  }
}
