package com.evolution.resourcepool

import cats.Applicative
import cats.effect.Resource

private[resourcepool] object ResourceHelper {

  implicit class OpsResourceHelper[F[_], A](val self: F[A]) extends AnyVal {
    def toResource(implicit F: Applicative[F]): Resource[F, A] = Resource.eval(self)
  }
}
