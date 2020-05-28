package stm.project
import java.io.{BufferedWriter, FileWriter}
import stm.projec.Trip
import org.kishore.scala.mcit.Route
import org.scala.mcit.Calendar
import scala.io.Source

object EnrichedTrip extends App {

  val outputFile = new BufferedWriter(new FileWriter("/home/bd-user/Desktop/Project/Enriched_trip.csv"))

  val tripList: List[Trip] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/trips.txt").getLines().toList.tail.map(Trip(_))
  val routeList: List[Route] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/routes.txt").getLines().toList.tail.map(Route(_))
  val calendarList: List[Calendar] = Source.fromFile("/home/bd-user/Downloads/gtfs_stm/calendar.txt").getLines().toList.tail.map(Calendar(_))
 val tripRoute: List[JoinOutput] = new StaticMapJoin[Trip, Route](m=> m.route_id.toString)(n=>n.route_id.toString).join(tripList, routeList)
//here there nestedloop join between calendar and Trip
    val enrichedTrip = new GenericNestedLoopJoin[Calendar, JoinOutput]((m, n) => m.service_id == n.left.asInstanceOf[Trip].service_id).join(calendarList, tripRoute)

// We got the output in text format now we are going to convert into csvformat

    val output = enrichedTrip.map(joinOutput => {
      //here we are convert the trip text format to csv format g
      val t = Trip.toCsv(joinOutput.right.getOrElse("0" ).asInstanceOf[JoinOutput].left.asInstanceOf[Trip])
      //here we are convert the Route text format to csv format
      val r = Route.toCsv(joinOutput.right.getOrElse("0").asInstanceOf[JoinOutput].right.getOrElse(" ").asInstanceOf[Route])
      //here we are convert the Calendar text format to csv format
      val c = Calendar.toCsv(joinOutput.left.asInstanceOf[Calendar])
      //by to make all list joins
      t+","r+","+c
    }
      )
  ///writing the output file to outputfile
  for (i <- output) {
    outputFile.newLine()
    outputFile.write(i)
  }
  outputFile.close()
  
  
  
  
  
  
  
  

}

