package com.evolution.resourcepool

import cats.effect.syntax.all.*
import cats.effect.{Concurrent, Deferred, IO, Ref, Resource, Temporal}
import cats.syntax.all.*
import com.evolution.resourcepool.util.IOSuite.*
import com.evolution.resourcepool.ResourcePool.implicits.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class ResourcePoolTest extends AsyncFunSuite with Matchers {
  type IOResource[+A] = Resource[IO, A]
  test("handle invalid `maxSize`") {
    ()
      .pure[IOResource]
      .toResourcePool(
        maxSize = 1,
        expireAfter = 1.day,
      )
      .use { pool =>
        pool
          .resource
          .use { _.pure[IO] }
      }
      .run()
  }

  test("allocate on demand and release on shutdown") {

    sealed trait Action

    object Action {
      case object Acquire extends Action
      case object Release extends Action
    }

    val result = for {
      ref     <- Ref[IO].of(List.empty[Action])
      add      = (a: Action) => ref.update { a :: _ }
      result  <- ResourcePool
        .of(
          maxSize = 1,
          expireAfter = 1.day,
          discardTasksOnRelease = false,
          resource = _ => Resource.make {
            add(Action.Acquire)
          } { _ =>
            add(Action.Release)
          }
        )
        .allocated
      (pool, release) = result
      actions <- ref.get
      _       <- IO { actions.reverse shouldEqual List.empty }
      _       <- pool.resource.use { _ =>
        for {
          actions <- ref.get
          result  <- IO { actions.reverse shouldEqual List(Action.Acquire) }
        } yield result
      }
      _       <- pool.resource.use { _.pure[IO] }
      actions <- ref.get
      _       <- IO { actions.reverse shouldEqual List(Action.Acquire) }
      _       <- release
      actions <- ref.get
      _       <- IO { actions.reverse shouldEqual List(Action.Acquire, Action.Release) }
    } yield {}

    result.run()
  }

  test("allocate multiple resources in parallel as long as it does not exceed `maxSize`") {
    val resource = for {
      deferred0 <- Deferred[IO, Unit].toResource
      deferred1 <- Deferred[IO, Unit].toResource
      deferred2 <- Deferred[IO, Unit].toResource
      deferreds  = List(deferred0, deferred1)
      ref       <- Ref[IO].of(deferreds).toResource
      pool      <- {
        val result = for {
          result <- ref.modify {
            case a :: as => (as, a.some)
            case as      => (as, none)
          }
          result <- result.foldMapM { _.complete(()).void }
          _      <- deferred2.get
        } yield result
        result
          .toResource
          .toResourcePool(
            maxSize = 2,
            expireAfter = 1.day)
      }
    } yield {
      for {
        fiber0 <- pool.resource.use { _.pure[IO] }.start
        fiber1 <- pool.resource.use { _.pure[IO] }.start
        _      <- deferreds.foldMapM { _.get }
        _      <- deferred2.complete(())
        _      <- fiber0.join
        _      <- fiber1.join
      } yield {}
    }

    resource
      .use(identity)
      .run()
  }

  test("fail after being released") {
    val result = for {
      result          <- ()
        .pure[IOResource]
        .toResourcePool(
          maxSize = 2,
          expireAfter = 1.day)
        .allocated
      (pool, release)  = result
      _               <- release
      result          <- pool.get.attempt
      _               <- IO { result shouldEqual ResourcePool.ReleasedError.asLeft }
    } yield {}
    result.run()
  }

  test("release gracefully") {
    val result = for {
      ref              <- Ref[IO].of(0)
      result           <- Resource
        .release { ref.update { _ + 1 } }
        .toResourcePool(
          maxSize = 2,
          expireAfter = 1.day)
        .allocated
      (pool, release0)  = result
      result           <- pool.get
      (_, release1)     = result
      result           <- pool.get
      (_, release2)     = result
      fiber0           <- pool
        .resource
        .use { _.pure[IO] }
        .start
      result           <- fiber0
        .join
        .timeout(10.millis)
        .attempt
      _                <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
      _                <- release1
      _                <- fiber0.joinWithNever
      fiber1           <- release0.start
      result           <- fiber1
        .join
        .timeout(10.millis)
        .attempt
      _                <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
      _                <- release2
      _                <- fiber1.joinWithNever
      result           <- ref.get
      _                <- IO { result shouldEqual 2 }
    } yield {}
    result.run()
  }

  test("release empty pool") {
    val result = for {
      ref    <- Ref[IO].of(0)
      _      <- ref
        .update { _ + 1 }
        .toResource
        .toResourcePool(
          maxSize = 2,
          expireAfter = 1.day)
        .use { _ => ().pure[IO] }
      result <- ref.get
      _      <- IO { result shouldEqual 0 }
    } yield {}
    result.run()
  }

  test("propagate release errors") {
    val error = new RuntimeException("error") with NoStackTrace
    val result = for {
      deferred         <- Deferred[IO, Unit]
      ref              <- Ref[IO].of(List(error.raiseError[IO, Unit], deferred.complete(()).void))
      result           <- Resource
        .release {
          ref
            .modify {
              case a :: as => (as, a)
              case as      => (as, ().pure[IO])
            }
            .flatten
        }
        .toResourcePool(
          maxSize = 2,
          expireAfter = 1.day)
        .allocated
      (pool, release0)  = result
      result           <- pool.get
      (_, release1)     = result
      _                <- pool.resource.use { _.pure[IO] }
      _                <- release1
      result           <- release0.attempt
      _                <- IO { result shouldEqual error.asLeft }
      _                <- deferred.get
      result           <- ref.get
      _                <- IO { result shouldEqual List.empty }
    } yield {}
    result.run()
  }

  test("expire after use") {
    val result = for {
      ref0      <- Ref[IO].of(0).toResource
      ref1      <- Ref[IO].of(0).toResource
      deferred0 <- Deferred[IO, Unit].toResource
      deferred1 <- Deferred[IO, Unit].toResource
      pool      <- Resource
        .make {
          for {
            a <- ref0.update { _ + 1 }
            _ <- deferred0.complete(())
          } yield a
        } { _ =>
          for {
            a <- ref1.update { _ + 1 }
            _ <- deferred1.complete(())
          } yield a
        }
        .toResourcePool(
          maxSize = 5,
          expireAfter = 10.millis)
      _        <- Concurrent[IO].background {
        pool
          .resource
          .use { _ => IO.sleep(10.millis) }
          .foreverM
          .void
      }
    } yield {
      for {
        _ <- deferred0.get
        _ <- pool.resource.use { _.pure[IO] }
        _ <- deferred1.get
        a <- ref0.get
        _ <- IO { a shouldEqual 2 }
        a <- ref1.get
        _ <- IO { a shouldEqual 1 }
      } yield {}
    }
    result
      .use(identity)
      .run()
  }

  test("not exceed `maxSize`") {
    val maxSize = 2
    val resource = for {
      ref  <- Ref[IO].of(0).toResource
      pool <- ref
        .update { _ + 1 }
        .toResource
        .toResourcePool(
          maxSize = maxSize,
          expireAfter = 1.day)
    } yield {
      for {
        _      <- pool
          .resource
          .use { _ => Temporal[IO].sleep(1.millis) }
          .parReplicateA(maxSize * 100)
          .map { _.combineAll }
        result <- ref.get
        _      <- IO { result shouldEqual maxSize }
      } yield {}
    }

    resource
      .use(identity)
      .run()
  }

  test("not exceed `maxSize` with partitioned pool") {
    val maxSize = 10
    val resource = for {
      ref  <- Ref[IO].of(List.empty[String]).toResource
      pool <- ResourcePool.of(
        maxSize = maxSize,
        partitions = 3,
        expireAfter = 1.day,
        discardTasksOnRelease = false,
        resource = id => ref.update { id :: _ }.toResource
      )
    } yield {
      for {
        _      <- pool
          .resource
          .use { _ => Temporal[IO].sleep(1.millis) }
          .parReplicateA(maxSize * 100)
          .map { _.combineAll }
        result <- ref.get
        _      <- IO { result.size shouldEqual maxSize }
        _      <- IO { result.toSet shouldEqual Set("2-3", "1-2", "1-0", "0-2", "1-1", "0-0", "2-2", "0-1", "2-0", "2-1") }
      } yield {}
    }

    resource
      .use(identity)
      .run()
  }

  test("resource allocation fails after some time") {
    val error = new RuntimeException("error") with NoStackTrace
    val result = for {
      deferred <- Deferred[IO, Throwable].toResource
      pool     <- ResourcePool.of(
        maxSize = 1,
        expireAfter = 1.day,
        discardTasksOnRelease = false,
        resource = _ => {
          for {
            a <- deferred.get.toResource
            a <- a.raiseError[IO, Unit].toResource
          } yield a
        }
      )
    } yield {
      for {
        fiber0 <- pool
          .resource
          .use { _.pure[IO] }
          .start
        result <- fiber0
          .joinWithNever
          .timeout(10.millis)
          .attempt
        _      <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
        fiber1 <- pool
          .resource
          .use { _.pure[IO] }
          .start
        result <- fiber1
          .joinWithNever
          .timeout(10.millis)
          .attempt
        _      <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
        _      <- deferred.complete(error)
        result <- fiber0.joinWithNever.attempt
        _      <- IO { result shouldEqual error.asLeft }
        result <- fiber1.joinWithNever.attempt
        _      <- IO { result shouldEqual error.asLeft }
      } yield {}
    }
    result
      .use(identity)
      .run()
  }

  test("cancel `get` while allocating resource") {
    IO
      .never[Unit]
      .toResource
      .toResourcePool(
        maxSize = 1,
        expireAfter = 1.day,
      )
      .use { pool =>
        for {
          fiber0 <- pool
            .resource
            .use { _.pure[IO] }
            .start
          result <- fiber0
            .joinWithNever
            .timeout(10.millis)
            .attempt
          _      <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
          fiber1 <- pool
            .resource
            .use { _.pure[IO] }
            .start
          _      <- fiber0.cancel
          result <- fiber1
            .joinWithNever
            .attempt
          _      <- IO { result shouldEqual ResourcePool.CancelledError.asLeft }
        } yield {}
      }
      .run()
  }

  test("cancel `get` while allocating resource, maxSize = 2") {
    val result = for {
      ref  <- Ref[IO]
        .of(IO.never[Unit].some)
        .toResource
      pool <- ref
        .getAndSet(none)
        .flatMap { _.foldA }
        .toResource
        .toResourcePool(
          maxSize = 2,
          expireAfter = 1.day,
        )
    } yield {
      for {
        fiber0 <- pool
          .resource
          .use { _.pure[IO] }
          .start
        result <- fiber0
          .joinWithNever
          .timeout(10.millis)
          .attempt
        _      <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
        _      <- pool
          .resource
          .use { _.pure[IO] }
        _      <- fiber0.cancel
      } yield {}
    }
    result
      .use(identity)
      .run()
  }

  test("cancel `get` while waiting in queue") {
    val result = for {
      deferred0 <- Deferred[IO, Unit].toResource
      pool      <- ()
        .pure[IOResource]
        .toResourcePool(
          maxSize = 1,
          expireAfter = 1.day)
    } yield {
      for {
        fiber0 <- pool
          .resource
          .use { _ =>
            deferred0
              .complete(())
              .productR { IO.never[Unit] }
          }
          .start
        _      <- deferred0.get
        fiber1 <- pool
          .get
          .start
        result <- fiber1
          .joinWithNever
          .timeout(10.millis)
          .attempt
        _      <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
        _      <- fiber1.cancel
        _      <- fiber0.cancel
      } yield {}
    }
    result
      .use(identity)
      .run()
  }

  test("cancel `resource` while waiting in queue") {
    val result = for {
      deferred0 <- Deferred[IO, Unit].toResource
      pool      <- ()
        .pure[IOResource]
        .toResourcePool(
          maxSize = 1,
          expireAfter = 1.day)
    } yield {
      for {
        fiber0 <- pool
          .resource
          .use { _ =>
            deferred0
              .complete(())
              .productR { IO.never[Unit] }
          }
          .start
        _      <- deferred0.get
        fiber1 <- pool
          .resource
          .use { _ => IO.never[Unit] }
          .start
        result <- fiber1
          .joinWithNever
          .timeout(10.millis)
          .attempt
        _      <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
        _      <- fiber1.cancel
        _      <- fiber0.cancel
      } yield {}
    }
    result
      .use(identity)
      .run()
  }

  test("cancel `resource.use") {
    val result = for {
      deferred0 <- Deferred[IO, Unit]
      deferred1 <- Deferred[IO, Unit]
      result    <- deferred0
        .complete(())
        .productR { deferred1.get }
        .toResource
        .toResourcePool(
          maxSize = 1,
          expireAfter = 1.day)
        .allocated
      (pool, release) = result
      fiber0    <- pool
        .resource
        .use { _ => IO.never }
        .start
      result    <- fiber0
        .joinWithNever
        .timeout(10.millis)
        .attempt
      _         <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
      _         <- deferred0.get
      fiber1    <- fiber0.cancel.start
      _         <- deferred1.complete(())
      _         <- fiber1.join
      _         <- release
    } yield {}
    result.run()
  }

  test("release before resource allocation is completed") {
    val result = for {
      deferred <- Deferred[IO, Unit]
      result   <- deferred
        .get
        .toResource
        .toResourcePool(
          maxSize = 1,
          expireAfter = 1.day)
        .allocated
      (pool, release) = result
      fiber0   <- pool
        .resource
        .use { _.pure[IO] }
        .start
      result   <- fiber0
        .joinWithNever
        .timeout(10.millis)
        .attempt
      _        <- IO { result should matchPattern { case Left(_: TimeoutException) => } }

      fiber1   <- release.start
      result   <- fiber1
        .joinWithNever
        .timeout(10.millis)
        .attempt
      _        <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
      _        <- deferred.complete(())
      _        <- fiber0.joinWithNever
      _        <- fiber1.joinWithNever
    } yield {}
    result.run()
  }

  test("use limited number of resources in pool in order to expire not needed") {

    sealed trait Action

    object Action {
      case object Allocate extends Action
      case object Release extends Action
    }

    val result = for {
      ref  <- Ref[IO].of(List.empty[Action]).toResource
      add   = (action: Action) => ref.update { action :: _ }
      pool <- Resource
        .make {
          add(Action.Allocate)
        } { _ =>
          add(Action.Release)
        }
        .toResourcePool(
          maxSize = 5,
          expireAfter = 10.millis)
    } yield {
      val job = pool
        .resource
        .use { _ => IO.sleep(1.millis) }
        .foreverM
        .void

      def actionsOf(size: Int) = {
        0.tailRecM { count =>
          ref
            .get
            .flatMap { actions =>
              if (actions.size >= size || count >= 10) {
                actions
                  .reverse
                  .asRight[Int]
                  .pure[IO]
              } else {
                IO
                  .sleep(10.millis)
                  .as { (count + 1).asLeft[Unit] }
              }
            }
        }
      }

      for {
        fiber0  <- job.start
        fiber1  <- job.start
        actions <- actionsOf(2)
        _       <- IO { actions shouldEqual List(Action.Allocate, Action.Allocate) }
        _       <- fiber1.cancel
        actions <- actionsOf(3)
        _       <- IO { actions shouldEqual List(Action.Allocate, Action.Allocate, Action.Release) }
        _       <- fiber0.cancel
      } yield {}
    }
    result
      .use(identity)
      .run()
  }

  test("discard tasks on release") {
    val result = for {
      result   <- ()
        .pure[IOResource]
        .toResourcePool(
          maxSize = 1,
          expireAfter = 1.day,
          discardTasksOnRelease = true)
        .allocated
      (pool, release) = result
      fiber     = pool
        .resource
        .use { _ => IO.never[Unit] }
        .start
      fiber0   <- fiber
      result   <- fiber0
        .joinWithNever
        .timeout(10.millis)
        .attempt
      _        <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
      fiber1   <- fiber
      result   <- fiber1
        .joinWithNever
        .timeout(10.millis)
        .attempt
      _        <- IO { result should matchPattern { case Left(_: TimeoutException) => } }
      fiber2   <- release.start
      result   <- fiber1
        .joinWithNever
        .attempt
      _        <- IO { result shouldEqual ResourcePool.ReleasedError.asLeft }
      _        <- fiber0.cancel
      _        <- fiber2.joinWithNever
    } yield {}
    result.run()
  }
}
