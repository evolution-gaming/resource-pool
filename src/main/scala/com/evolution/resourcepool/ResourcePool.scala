package com.evolution.resourcepool

import cats.Functor
import cats.effect.{Async, Resource, Temporal}
import cats.effect.kernel.{Deferred, Ref}
import cats.effect.syntax.all._
import cats.syntax.all._

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

trait ResourcePool[F[_], A] {
  import ResourcePool.Release

  def get: F[(A, Release[F])]
}

/**
  * TODO
  * * partitioning
  * * cancellable
  */
object ResourcePool {

  type Release[F[_]] = F[Unit]

  type Id = String

  def of[F[_]: Async: Temporal, A](
    maxSize: Int,
    expireAfter: FiniteDuration,
    resource: Id => Resource[F, A]
  ): Resource[F, ResourcePool[F, A]] = {

    def apply(maxSize: Int) = {

      type Id = Long
      type Ids = List[Id]
      type Release = ResourcePool.Release[F]
      type Result = (A, Release)
      type Task = Deferred[F, Either[Throwable, (Id, Entry)]]
      type Tasks = Queue[Task]

      def now = Temporal[F].realTime

      final case class Entry(value: A, release: F[Unit], timestamp: FiniteDuration)

      sealed trait State

      object State {

        def empty: State = {
          Allocated(
            id = 0L,
            entries = Map.empty,
            stage = Allocated.Stage.free(List.empty),
            releasing = Set.empty)
        }

        final case class Allocated(
          id: Long,
          entries: Map[Id, Option[Entry]],
          stage: Allocated.Stage,
          releasing: Set[Id]
        ) extends State

        object Allocated {

          sealed trait Stage

          object Stage {

            def free(ids: Ids): Stage = Free(ids)

            def busy(task: Task): Stage = Busy(Queue(task))

            final case class Free(ids: Ids) extends Stage

            final case class Busy(tasks: Tasks) extends Stage
          }
        }

        final case class Released(
          allocated: Set[Id],
          releasing: Set[Id],
          tasks: Tasks,
          released: Deferred[F, Either[Throwable, Unit]]
        ) extends State
      }

      for {
        ref <- Resource
          .make {
            Ref[F].of { State.empty }
          } { ref =>
            0.tailRecM { count =>
              ref
                .access
                .flatMap {
                  case (state: State.Allocated, set) =>
                    Deferred
                      .apply[F, Either[Throwable, Unit]]
                      .flatMap { released =>

                        def apply(allocated: Set[Id], releasing: Set[Id], tasks: Tasks)(effect: => F[Unit]) = {
                          set
                            .apply { State.Released(allocated = allocated, releasing = releasing, tasks, released) }
                            .flatMap {
                              case true  =>
                                for {
                                  result <- {
                                    if (allocated.isEmpty && releasing.isEmpty) {
                                      released
                                        .complete(().asRight)
                                        .void
                                    } else {
                                      effect.productR {
                                        released
                                          .get
                                          .rethrow
                                      }
                                    }
                                  }
                                } yield {
                                  result.asRight[Int]
                                }
                              case false =>
                                (count + 1)
                                  .asLeft[Unit]
                                  .pure[F]
                            }
                            .uncancelable
                        }

                        state.stage match {
                          case stage: State.Allocated.Stage.Free =>
                            val (entries, releasing, release) = stage
                              .ids
                              .foldLeft((state.entries, state.releasing, ().pure[F])) {
                                case ((entries, releasing, release), id) =>
                                  entries
                                    .get(id)
                                    .fold {
                                      (entries, releasing, release)
                                    } {
                                      case Some(entry) =>
                                        (entries - id, releasing + id, release.productR(entry.release))
                                      case None        =>
                                        (entries, releasing, release)
                                    }
                              }

                            apply(
                              allocated = entries.keySet,
                              releasing = releasing,
                              Queue.empty
                            ) {
                              release
                            }

                          case stage: State.Allocated.Stage.Busy =>
                            apply(
                              allocated = state.entries.keySet,
                              releasing = state.releasing,
                              stage.tasks
                            ) {
                              ().pure[F]
                            }
                        }
                      }

                  case (state: State.Released, _) =>
                    state
                      .released
                      .get
                      .rethrow
                      .map { _.asRight[Int] }
                }
            }
          }
        _   <- Async[F].background {
          val interval = expireAfter / 10
          for {
            _ <- Temporal[F].sleep(expireAfter)
            a <- Async[F].foreverM[Unit, Unit] {
              for {
                now       <- now
                threshold  = now - expireAfter
                result    <- 0.tailRecM { count =>
                  ref
                    .access
                    .flatMap {
                      case (state: State.Allocated, set) =>
                        state.stage match {
                          case stage: State.Allocated.Stage.Free =>
                            val (ids, entries, releasing, release) = stage
                              .ids
                              .foldLeft((List.empty[Id], state.entries, state.releasing, ().pure[F])) {
                                case ((ids, entries, releasing, release), id) =>
                                  entries
                                    .get(id)
                                    .fold {
                                      (ids, entries, releasing, release)
                                    } {
                                      case Some(entry) =>
                                        if (entry.timestamp < threshold) {
                                          (ids, entries - id, releasing + id, release.productR(entry.release))
                                        } else {
                                          (id :: ids, entries, releasing, release)
                                        }
                                      case None        =>
                                        (ids, entries, releasing, release)
                                    }
                              }

                            if (stage.ids.sizeCompare(ids) == 0) {
                              ()
                                .asRight[Int]
                                .pure[F]
                            } else {
                              set
                                .apply {
                                  state.copy(
                                    entries = entries,
                                    stage = stage.copy(ids = ids.reverse),
                                    releasing = releasing)
                                }
                                .flatMap {
                                  case true  =>
                                    release.map { _.asRight[Int] }
                                  case false =>
                                    (count + 1)
                                      .asLeft[Unit]
                                      .pure[F]
                                }
                                .uncancelable
                            }

                          case _: State.Allocated.Stage.Busy =>
                            ()
                              .asRight[Int]
                              .pure[F]
                        }

                      case (_: State.Released, _) =>
                        ()
                          .asRight[Int]
                          .pure[F]
                    }
                }
                _         <- Temporal[F].sleep(interval)
              } yield result
            }
          } yield a
        }
      } yield {
        new ResourcePool[F, A] {
          def get = {

            def releaseOf(id: Id, entry: Entry): Release = {
              for {
                timestamp <- now
                entry     <- entry.copy(timestamp = timestamp).pure[F]
                result    <- 0.tailRecM { counter =>
                  ref
                    .access
                    .flatMap {
                      case (state: State.Allocated, set) =>
                        def apply(stage: State.Allocated.Stage)(effect: => F[Unit]) = {
                          set
                            .apply {
                              state.copy(
                                entries = state.entries.updated(id, entry.some),
                                stage = stage)
                            }
                            .flatMap {
                              case true  => effect.map { _.asRight[Int] }
                              case false => (counter + 1).asLeft[Unit].pure[F]
                            }
                            .uncancelable
                        }

                        state
                          .stage match {
                          case stage: State.Allocated.Stage.Free =>
                            apply {
                              stage.copy(ids = id :: stage.ids)
                            } {
                              ().pure[F]
                            }
                          case stage: State.Allocated.Stage.Busy =>
                            stage
                              .tasks
                              .dequeueOption
                              .fold {
                                apply(
                                  State.Allocated.Stage.free(List(id))
                                ) {
                                  ().pure[F]
                                }
                              } { case (task, tasks) =>
                                apply(
                                  stage.copy(tasks = tasks)
                                ) {
                                  task
                                    .complete((id, entry).asRight)
                                    .void
                                }
                              }
                        }

                      case (state: State.Released, set) =>

                        def apply(
                          allocated: Set[Id],
                          releasing: Set[Id],
                          tasks: Tasks,
                        )(effect: F[Unit]) = {
                          set
                            .apply {
                              state.copy(
                                allocated = allocated,
                                releasing = releasing,
                                tasks = tasks)
                            }
                            .flatMap {
                              case true  => effect.map { _.asRight[Int] }
                              case false => (counter + 1).asLeft[Unit].pure[F]
                            }
                            .uncancelable
                        }

                        state
                          .tasks
                          .dequeueOption
                          .fold {
                            apply(
                              state.allocated - id,
                              state.releasing + id,
                              state.tasks
                            ) {
                              entry.release
                            }
                          } { case (task, tasks) =>
                            apply(
                              state.allocated,
                              state.releasing,
                              tasks
                            ) {
                              task
                                .complete((id, entry).asRight)
                                .void
                            }
                          }

                    }
                }
              } yield result
            }

            0.tailRecM { count =>
              ref
                .access
                .flatMap {
                  case (state: State.Allocated, set) =>

                    def apply[X](state: State.Allocated)(effect: => F[X]) = {
                      set
                        .apply(state)
                        .flatMap {
                          case true  =>
                            effect.map { _.asRight[Int] }
                          case false =>
                            (count + 1)
                              .asLeft[X]
                              .pure[F]
                        }
                        .uncancelable
                    }

                    state.stage match {
                      case stage: State.Allocated.Stage.Free =>
                        stage.ids match {
                          case id :: ids =>
                            state
                              .entries
                              .get(id)
                              .fold {
                                IllegalStateError(s"entry is not found, id: $id").raiseError[F, Either[Int, (A, Release)]]
                              } { entry =>
                                entry.fold {
                                  IllegalStateError(s"entry is not defined, id: $id").raiseError[F, Either[Int, (A, Release)]]
                                } { entry0 =>
                                  now.flatMap { timestamp =>
                                    val entry = entry0.copy(timestamp = timestamp)
                                    set
                                      .apply {
                                        state.copy(
                                          stage = stage.copy(ids),
                                          entries = state.entries.updated(
                                            id,
                                            entry0
                                              .copy(timestamp = timestamp)
                                              .some))
                                      }
                                      .map {
                                        case true  => (entry0.value, releaseOf(id, entry)).asRight[Int]
                                        case false => (count + 1).asLeft[Result]
                                      }
                                  }
                                }
                              }

                          case Nil =>
                            val entries = state.entries
                            if (entries.sizeCompare(maxSize) < 0) {
                              val id = state.id
                              apply {
                                state.copy(
                                  id = id + 1,
                                  entries = state.entries.updated(id, none))
                              } {
                                resource
                                  .apply(id.toString)
                                  .allocated
                                  .attempt
                                  .flatMap {
                                    case Right((value, release)) =>
                                      for {
                                        timestamp <- now
                                        entry = Entry(
                                          value = value,
                                          release = {
                                            val result = for {
                                              result <- release.attempt
                                              result <- 0.tailRecM { count =>
                                                ref
                                                  .access
                                                  .flatMap {
                                                    case (state: State.Allocated, set) =>
                                                      set
                                                        .apply { state.copy(releasing = state.releasing - id) }
                                                        .map {
                                                          case true  => ().asRight[Int]
                                                          case false => (count + 1).asLeft[Unit]
                                                        }

                                                    case (state: State.Released, set) =>
                                                      val releasing = state.releasing - id
                                                      set
                                                        .apply {
                                                          state.copy(releasing = releasing)
                                                        }
                                                        .flatMap {
                                                          case true  =>
                                                            val result1 = result match {
                                                              case Right(a) =>
                                                                if (releasing.isEmpty && state.allocated.isEmpty) {
                                                                  state
                                                                    .released
                                                                    .complete(a.asRight)
                                                                    .void
                                                                } else {
                                                                  ().pure[F]
                                                                }
                                                              case Left(a)  =>
                                                                state
                                                                  .released
                                                                  .complete(a.asLeft)
                                                                  .void
                                                            }
                                                            result1.map { _.asRight[Int] }
                                                          case false =>
                                                            (count + 1)
                                                              .asLeft[Unit]
                                                              .pure[F]
                                                        }
                                                        .uncancelable
                                                  }
                                              }
                                            } yield result
                                            result
                                              .start
                                              .void
                                          },
                                          timestamp = timestamp)
                                        _ <- ref
                                          .access
                                          .flatMap {
                                            case (state: State.Allocated, set) =>
                                              set
                                                .apply { state.copy(entries = state.entries.updated(id, entry.some)) }
                                                .map {
                                                  case true  => ().asRight[Int]
                                                  case false => (count + 1).asLeft[Unit]
                                                }
                                            case (_: State.Released, _)        =>
                                              ()
                                                .asRight[Int]
                                                .pure[F]
                                          }
                                      } yield {
                                        (value, releaseOf(id, entry))
                                      }
                                    case Left(a)                 =>
                                      0
                                        .tailRecM { count =>
                                          ref
                                            .access
                                            .flatMap {
                                              case (state: State.Allocated, set) =>

                                                val entries = state.entries - id

                                                def apply(stage: State.Allocated.Stage)(effect: => F[Unit]) = {
                                                  set
                                                    .apply {
                                                      state.copy(
                                                        entries = entries,
                                                        stage = stage)
                                                    }
                                                    .flatMap {
                                                      case true  =>
                                                        effect.map { _.asRight[Int] }
                                                      case false =>
                                                        (count + 1)
                                                          .asLeft[Unit]
                                                          .pure[F]
                                                    }
                                                    .uncancelable
                                                }

                                                if (entries.isEmpty) {
                                                  state.stage match {
                                                    case stage: State.Allocated.Stage.Free =>
                                                      apply(stage) { ().pure[F] }
                                                    case stage: State.Allocated.Stage.Busy =>
                                                      apply {
                                                        State.Allocated.Stage.free(List.empty)
                                                      } {
                                                        stage
                                                          .tasks
                                                          .foldMapM { task =>
                                                            task
                                                              .complete(a.asLeft)
                                                              .void
                                                          }
                                                      }
                                                  }
                                                } else {
                                                  apply(stage) { ().pure[F] }
                                                }

                                              case (state: State.Released, set) =>

                                                val allocated = state.allocated - id

                                                def apply(tasks: Tasks)(effect: => F[Unit]) = {
                                                  set
                                                    .apply {
                                                      state.copy(
                                                        allocated = allocated,
                                                        tasks = tasks)
                                                    }
                                                    .flatMap {
                                                      case true  =>
                                                        effect.map { _.asRight[Int] }
                                                      case false =>
                                                        (count + 1)
                                                          .asLeft[Unit]
                                                          .pure[F]
                                                    }
                                                    .uncancelable
                                                }

                                                if (allocated.isEmpty) {
                                                  apply(Queue.empty) {
                                                    state
                                                      .tasks
                                                      .foldMapM { task =>
                                                        task
                                                          .complete(a.asLeft)
                                                          .void
                                                      }
                                                      .productR {
                                                        if (state.releasing.isEmpty) {
                                                          state
                                                            .released
                                                            .complete(().asRight)
                                                            .void
                                                        } else {
                                                          ().pure[F]
                                                        }
                                                      }
                                                  }
                                                } else {
                                                  apply(state.tasks) { ().pure[F] }
                                                }
                                            }
                                        }
                                        .productR { a.raiseError[F, Result] }
                                  }
                              }
                            } else {
                              Deferred
                                .apply[F, Either[Throwable, (Id, Entry)]]
                                .flatMap { task =>
                                  set
                                    .apply { state.copy(stage = State.Allocated.Stage.busy(task)) }
                                    .flatMap {
                                      case true  =>
                                        task
                                          .get
                                          .rethrow
                                          .map { case (id, entry) =>
                                            (entry.value, releaseOf(id, entry)).asRight[Int]
                                          }
                                      case false =>
                                        (count + 1)
                                          .asLeft[Result]
                                          .pure[F]
                                    }
                                    .uncancelable
                                }
                            }
                        }

                      case stage: State.Allocated.Stage.Busy =>
                        Deferred
                          .apply[F, Either[Throwable, (Id, Entry)]]
                          .flatMap { task =>
                            set
                              .apply { state.copy(stage = stage.copy(stage.tasks.enqueue(task))) }
                              .flatMap {
                                case true  =>
                                  task
                                    .get
                                    .rethrow
                                    .map { case (id, entry) =>
                                      (entry.value, releaseOf(id, entry)).asRight[Int]
                                    }
                                case false =>
                                  (count + 1)
                                    .asLeft[Result]
                                    .pure[F]
                              }
                              .uncancelable
                          }
                    }

                  case (_: State.Released, _) =>
                    ReleasedError.raiseError[F, Either[Int, Result]]
                }
            }
          }
        }
      }
    }

    apply(maxSize.max(1))
  }


  final case object ReleasedError extends RuntimeException("released") with NoStackTrace

  final case class IllegalStateError(msg: String) extends RuntimeException(msg) with NoStackTrace


  implicit class ResourcePoolOps[F[_], A](val self: ResourcePool[F, A]) extends AnyVal {

    def resource(implicit F: Functor[F]): Resource[F, A] = Resource(self.get)
  }

  object implicits {
    implicit class ResourceOpsResourcePool[F[_], A](val self: Resource[F, A]) extends AnyVal {
      def toResourcePool(
        maxSize: Int,
        expireAfter: FiniteDuration,
      )(implicit
        async: Async[F],
        temporal: Temporal[F],
      ): Resource[F, ResourcePool[F, A]] = {
        ResourcePool.of(maxSize, expireAfter, _ => self)
      }
    }
  }
}
