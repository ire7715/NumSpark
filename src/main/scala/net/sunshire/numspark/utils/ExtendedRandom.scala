package net.sunshire.numspark.utils

import scala.util.Random

object ExtendedRandom {
  def apply(delegation: Random) = new ExtendedRandom(delegation)

  /**
    * To implicitly convert Random to ExtendedRandom for some extended methods.
    */
  implicit def entend(delegation: Random) = ExtendedRandom(delegation)
}

class ExtendedRandom(delegation: Random) {
  /**
    * Choose from a Seq randomly.
    *
    * @param seq: Seq[T]; the sequence to be choose.
    * @return Option[T]; the chosen element, could be None when the sequence has no element.
    */
  def choice[T](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty) return None
    else Option(seq(delegation.nextInt(seq.length)))
  }
}