package net.sunshire.numspark.utils

import org.scalatest.FunSuite
import net.sunshire.numspark.utils.ExtendedRandom._
import scala.util.Random

class EntendedRandomSuite extends FunSuite {
  test("able to type cast from Random to ExtendedRandom") {
    def hasImplicit(random: Random)
    (implicit conversion: Random => ExtendedRandom = null): Boolean = conversion != null

    assert(hasImplicit(Random))
  }

  test("random choice") {
    val lowerBound = 1
    val upperBound = 10
    val sequence = lowerBound to upperBound
    val choice = Random.choice(sequence).get
    assert(choice >= lowerBound)
    assert(choice <= upperBound)
  }

  test("choice from empty set") {
    val sequence = Seq[Int]()
    val choiceOption = Random.choice(sequence)
    assert(choice.isEmpty)
  }
}
