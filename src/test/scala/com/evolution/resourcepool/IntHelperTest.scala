package com.evolution.resourcepool

import org.scalatest.funsuite.AnyFunSuite
import com.evolution.resourcepool.IntHelper._
import org.scalatest.matchers.should.Matchers

class IntHelperTest extends AnyFunSuite with Matchers {

  for {
    (value, divisor, result) <- List(
      (1, 2, List(1)),
      (1, 3, List(1)),
      (2, 3, List(1, 1)),
      (3, 3, List(1, 1, 1)),
      (3, 2, List(2, 1)),
      (5, 3, List(2, 2, 1)),
      (5, 2, List(3, 2)),
      (5, 4, List(1, 1, 1, 2)))
  } {
    test(s"value: $value, divisor: $divisor, result: $result") {
      value.divide(divisor) shouldEqual result
      result.sum shouldEqual value
      result.size shouldEqual divisor.min(value)
    }
  }
}
