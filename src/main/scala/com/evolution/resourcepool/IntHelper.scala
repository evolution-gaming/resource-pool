package com.evolution.resourcepool

import scala.annotation.tailrec

private[resourcepool] object IntHelper {

  implicit class IntOpsIntHelper(val self: Int) extends AnyVal {

    def divide(divisor: Int): List[Int] =
      if (divisor <= 0 || self <= 0)
        Nil
      else if (divisor >= self)
        List.fill(self)(1)
      else if (divisor == 1)
        List(self)
      else {
        val quotient = (self.toDouble / divisor).round.toInt

        @tailrec
        def loop(self: Int, value: Int, result: List[Int]): List[Int] =
          if (self > quotient && value > 1)
            loop(self - quotient, value - 1, quotient :: result)
          else
            (self :: result).reverse

        loop(self, divisor, List.empty)
      }
  }
}
