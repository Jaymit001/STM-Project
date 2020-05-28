package stm.project
import java.io.{BufferedWriter, FileWriter}
import com.sun.xml.internal.ws.client.sei.ResponseBuilder.Header
import stm.project.{Calendar, Route, Trip}
import scala.io.Source

object EnrichedTrip extends App {

  val outputFile = new BufferedWriter(new FileWriter("/home/bd-user/Desktop/Project/Enriched_trip.csv"))

  val tripList: List[Trip] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/trips.txt").getLines().toList.tail.map(Trip(_))
  val routeList: List[Route] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/routes.txt").getLines().toList.tail.map(Route(_))
  val calendarList: List[Calendar] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/calendar.txt").getLines().toList.tail.map(Calendar(_))
}

