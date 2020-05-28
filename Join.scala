package stm.project

trait Join[L,R,Q] {
  def join(a: List[L], b: List[R]): List[Q]
}

case class JoinOutput(left: Any, right: Option[Any])

class StaticMapJoin[L,R] (val joinCond: (L) => String ) (val joinCond1: (R) => String ) extends Join[L,R, JoinOutput] {

  override def join(a: List[L], b: List[R]): List[JoinOutput] = {
    val l: Map[String, R] = b.map(b => joinCond1(b) -> b).toMap
    a.filter(a => l.contains(joinCond(a))).map(a => JoinOutput(a, Some(l(joinCond(a)))))
  }
}
class GenericNestedLoopJoin[L, R](val joinCond: (L, R) => Boolean) extends Join[L, R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = for {
    l <- a
    r <- b
    if joinCond(l, r)
  } yield JoinOutput(l, Some(r))

}
