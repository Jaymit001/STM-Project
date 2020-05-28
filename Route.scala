package stm.project
case class Route(
                  route_id: Int,
                  agency_id: String,
                  route_short_name: String,
                  route_long_name: String,
                  route_type: String,
                  route_url: String,
                  route_color: String,
                  route_text_color: String
                )


object Route {
  def apply(csvLine: String): Route = {
    val p = csvLine.split(",",-1)
    new Route(p(0).toInt, p(1), p(2), p(3), p(4), p(5), p(6),p(7))
  }
  def toCsv(route: Route): String = {
    route.route_id + "," +
      route.agency_id + "," +
      route.route_short_name + "," +
      route.route_long_name + "," +
      route.route_type + "," +
      route.route_url + "," +
    route.route_color + "," +
      route.route_text_color
  }
}

