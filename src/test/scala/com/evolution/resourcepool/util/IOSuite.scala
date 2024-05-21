package com.evolution.resourcepool.util

import cats.Applicative
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.scalatest.Succeeded

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object IOSuite {
  val Timeout: FiniteDuration = 1.minute

  implicit val executor: ExecutionContextExecutor = ExecutionContext.global

  def runIO[A](io: IO[A], timeout: FiniteDuration = Timeout): Future[Succeeded.type] =
    io.timeout(timeout).as(Succeeded).unsafeToFuture()

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {

    def run(timeout: FiniteDuration = Timeout): Future[Succeeded.type] = runIO(self, timeout)
  }

  implicit class ResourceObjOps(val self: Resource.type) extends AnyVal {

    def release[F[_]: Applicative](release: F[Unit]): Resource[F, Unit] =
      Resource(((), release).pure[F])
  }
}
