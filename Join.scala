package stm.project

trait Join[L,R,Q] {
  def join(a: List[L], b: List[R]): List[Q]
}

case class JoinOutput(left: Any, right: Option[Any])
//StaticMapJoin is between left and right protion of mapping And also lookup on smalltable becausecommon element
class StaticMapJoin[L,R] (val joinCon: (L) => String ) (val joinCon1: (R) => String ) extends Join[L,R, JoinOutput] {

  override def join(a: List[L], b: List[R]): List[JoinOutput] = {
    val l: Map[String, R] = b.map(b => joinCon1(b) -> b).toMap
    a.filter(a => l.contains(joinCon(a))).map(a => JoinOutput(a, Some(l(joinCon(a)))))
  }
}

//This an nesetd loopjoin between two lists

class GenericNestedLoopJoin[L, R](val joinCond: (L, R) => Boolean) extends Join[L, R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = for {
    l <- a
    r <- b
    if joinCond(l, r)
  } yield JoinOutput(l, Some(r))

}
