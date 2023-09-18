package com.evolution.resourcepool

import scala.annotation.tailrec

object IntHelper {

  implicit class IntOpsIntHelper(val self: Int) extends AnyVal {

    def divide(value: Int): List[Int] = {
      if (value <= 0 || self <= 0) {
        List.empty
      } else if (value >= self) {
        List.fill(self)(1)
      } else if (value == 1) {
        List(self)
      } else {
        val quotient = (self.toDouble / value)
          .round
          .toInt

        @tailrec
        def loop(self: Int, value: Int, result: List[Int]): List[Int] = {
          if (self > quotient && value > 1) {
            loop(self - quotient, value - 1, quotient :: result)
          } else {
            self :: result
          }
        }

        loop(self, value, List.empty)
      }
    }
  }
}
