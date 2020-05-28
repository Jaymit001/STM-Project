package stm.project

trait Join[L,R,Q] {
  def join(a: List[L], b: List[R]): List[Q]
}

case class JoinOutput(left: Any, right: Option[Any])


class GenericNestedLoopJoin[L, R](val joinCond: (L, R) => Boolean) extends Join[L, R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = for {
    l <- a
    r <- b
    if joinCond(l, r)
  } yield JoinOutput(l, Some(r))

}