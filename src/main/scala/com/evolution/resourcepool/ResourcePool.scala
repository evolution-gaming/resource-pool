package com.evolution.resourcepool

import cats.{Functor, Parallel}
import cats.effect.{Async, Deferred, MonadCancel, MonadCancelThrow, Poll, Ref, Resource, Sync, Temporal}
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolution.resourcepool.IntHelper._

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

trait ResourcePool[F[_], A] {
  import ResourcePool.Release

  /** Returns the acquired resource, and a handle releasing it back to the pool.
    *
    * Calling the handle will not release resource itself, but just make it
    * available to be returned again, though resource may expire and be released
    * if it stays unused for long enough.
    *
    * The resource leak may occur if the release handle is never called.
    * Therefore it is recommended to use
    * [[ResourcePool.ResourcePoolOps#resource]] method instead, which will
    * return [[cats.effect.Resource]] calling the handle on release.
    */
  def get: F[(A, Release[F])]
}

object ResourcePool {

  type Release[F[_]] = F[Unit]

  type Id = String

  /** Same as [[of[F[_],A](maxSize:Int,partitions:Int*]], but number of partitions is
    * determined automatically by taking into account the number of available
    * processors and expected pool size.
    */
  def of[F[_]: Async: Parallel, A](
    maxSize: Int,
    expireAfter: FiniteDuration,
    resource: Id => Resource[F, A]
  ): Resource[F, ResourcePool[F, A]] = {

    def apply(maxSize: Int) = {
      for {
        cpus       <- Sync[F]
          .delay { Runtime.getRuntime.availableProcessors() }
          .toResource
        result     <- of(
          maxSize = maxSize,
          partitions = (maxSize / 100).min(cpus),
          expireAfter,
          resource)
      } yield result
    }

    apply(maxSize.max(1))
  }

  /** Creates a new pool with specified number of partitions.
    *
    * @param maxSize
    *   Maximum size of the whole pool.
    * @param partitions
    *   Number of paritions to be used. This number determines the count of the
    *   threads, that could access the pool in parallel, and also number of
    *   background processes removing the expiring entries.
    * @param expireAfter
    *   Duration after which the resource should be removed if unused.
    * @param resource
    *   Factory for creating the new resources. `Id` is a unique identifier of a
    *   resource that could be used, for example, for logging purposes.
    */
  def of[F[_]: Async: Parallel, A](
    maxSize: Int,
    partitions: Int,
    expireAfter: FiniteDuration,
    resource: Id => Resource[F, A]
  ): Resource[F, ResourcePool[F, A]] = {

    def apply(maxSize: Int, partitions: Int) = {

      def of(maxSize: Int)(resource: Id => Resource[F, A]) = {
        of0(
          maxSize,
          expireAfter,
          resource)
      }

      if (partitions <= 1) {
        of(maxSize)(resource)
      } else {
        for {
          ref    <- Ref[F].of(0).toResource
          values <- maxSize
            .divide(partitions)
            .zipWithIndex
            .parTraverse { case (maxSize, idx) => of(maxSize) { id => resource(s"$idx-$id") } }
          values <- values
            .toVector
            .pure[Resource[F, *]]
          length  = values.length
        } yield {
          new ResourcePool[F, A] {
            def get = {
              for {
                partition <- ref.modify { a =>
                  val b = a + 1
                  (
                    if (b < length) b else 0,
                    a
                  )
                }
                result    <- values
                  .apply(partition)
                  .get
              } yield result
            }
          }
        }
      }
    }

    apply(
      maxSize = maxSize.max(1),
      partitions = partitions.max(1))
  }

  private def of0[F[_]: Async, A](
    maxSize: Int,
    expireAfter: FiniteDuration,
    resource: Id => Resource[F, A]
  ): Resource[F, ResourcePool[F, A]] = {

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

      /** Resource pool is allocated.
        *
        * @param id
        *   Sequence number of a last resource allocated (used to generate an
        *   identifier for a next resource).
        * @param entries
        *   Allocated or allocating resources. `Some` means that resource is
        *   allocated, and `None` means allocating is in progress.
        * @param stage
        *   Represents a state of a pool, i.e. if it is fully busy, if there are
        *   free resources, and the tasks waiting for resources to be freed.
        * @param releasing
        *   List of ids being released because these have expired.
        */
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

          def busy(tasks: Tasks): Stage = Busy(tasks)

          /** There are free resources to use.
            *
            * @param ids
            *   List of ids from [[Allocated#Entries]] that are free to use. It
            *   could be equal to `Nil` if all resources are busy, but there
            *   are no tasks waiting in queue.
            */
          final case class Free(ids: Ids) extends Stage

          /** No more free resources to use, and tasks are waiting in queue.
            *
            * @param tasks
            *   List of tasks waiting for resources to free up.
            */
          final case class Busy(tasks: Tasks) extends Stage
        }
      }

      /** Resource pool is being released.
        *
        * @param allocated
        *   Allocated resources.
        * @param releasing
        *   List of ids being released (either because pool is releasing or
        *   because these expired earlier).
        * @param tasks
        *   The list of tasks left to be completed before the pool could be
        *   released.
        * @param released
        *   `Deferred`, which will be completed, when all the tasks are
        *   completed and all resources are released.
        */
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
                                    // the pool is empty now, we can safely release it
                                    released
                                      .complete(().asRight)
                                      .void
                                  } else {
                                    // the pool will be released elsewhere when all resources in `allocated` or
                                    // `releasing` get released
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
                          // glue `release` functions of all free resources together
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
      _ <- Async[F].background {
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

                          set
                            .apply {
                              state.copy(
                                entries   = entries,
                                stage     = stage.copy(ids = ids.reverse),
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
              _ <- Temporal[F].sleep(interval)
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
              result    <- ref
                .modify {
                  case state: State.Allocated =>

                    def stateOf(stage: State.Allocated.Stage) = {
                      state.copy(
                        entries = state.entries.updated(id, entry.some),
                        stage = stage)
                    }

                    state
                      .stage match {
                      case stage: State.Allocated.Stage.Free =>
                        (
                          stateOf(stage.copy(ids = id :: stage.ids)),
                          ().pure[F]
                        )
                      case stage: State.Allocated.Stage.Busy =>
                        stage
                          .tasks
                          .dequeueOption
                          .fold {
                            (
                              stateOf(State.Allocated.Stage.free(List(id))),
                              ().pure[F]
                            )
                          } { case (task, tasks) =>
                            (
                              stateOf(stage.copy(tasks = tasks)),
                              task
                                .complete((id, entry).asRight)
                                .void
                            )
                          }
                    }

                  case state: State.Released =>
                    state
                      .tasks
                      .dequeueOption
                      .fold {
                        (
                          state.copy(
                            allocated = state.allocated - id,
                            releasing = state.releasing + id),
                          entry.release
                        )
                      } { case (task, tasks) =>
                        (
                          state.copy(tasks = tasks),
                          task
                            .complete((id, entry).asRight)
                            .void
                        )
                      }
                }
                .flatten
                .uncancelable
            } yield result
          }

          0.tailRecM { count =>
            ref
              .access
              .flatMap {
                case (state: State.Allocated, set) =>

                  def apply[X](state: State.Allocated)(effect: Poll[F] => F[X]) = {
                    MonadCancel[F].uncancelable { poll =>
                      set
                        .apply(state)
                        .flatMap {
                          case true  =>
                            effect
                              .apply(poll)
                              .map { _.asRight[Int] }
                          case false =>
                            (count + 1)
                              .asLeft[X]
                              .pure[F]
                        }
                    }
                  }

                  def enqueue(tasks: Tasks) = {
                    Deferred
                      .apply[F, Either[Throwable, (Id, Entry)]]
                      .flatMap { task =>
                        apply {
                          state.copy(stage = State.Allocated.Stage.busy(tasks.enqueue(task)))
                        } { poll =>
                          poll
                            .apply { task.get }
                            .onCancel {
                              ref.update {
                                case state: State.Allocated =>
                                  state.stage match {
                                    case _: State.Allocated.Stage.Free     =>
                                      state
                                    case stage: State.Allocated.Stage.Busy =>
                                      state.copy(
                                        stage = stage.copy(
                                          tasks = stage
                                            .tasks
                                            .filter { _ ne task }))
                                  }

                                case state: State.Released =>
                                  state.copy(tasks =
                                    state
                                      .tasks
                                      .filter { _ ne task })
                              }
                            }
                            .rethrow
                            .map { case (id, entry) =>
                              (entry.value, releaseOf(id, entry))
                            }
                        }
                      }
                  }

                  state.stage match {
                    case stage: State.Allocated.Stage.Free =>
                      stage.ids match {
                        // there are free resources to use
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
                                  apply {
                                    state.copy(
                                      stage = stage.copy(ids),
                                      entries = state.entries.updated(
                                        id,
                                        entry0
                                          .copy(timestamp = timestamp)
                                          .some))
                                  } { _ =>
                                    (entry0.value, releaseOf(id, entry)).pure[F]
                                  }
                                }
                              }
                            }

                        // no free resources found
                        case Nil =>
                          val entries = state.entries
                          if (entries.sizeCompare(maxSize) < 0) {
                            // pool is not full, create a new resource
                            val id = state.id
                            apply {
                              state.copy(
                                id      = id + 1,
                                entries = state.entries.updated(id, none))
                            } { _ =>
                              resource
                                .apply(id.toString)
                                .allocated
                                .attempt
                                .flatMap {
                                  case Right((value, release)) =>
                                    // resource was allocated
                                    for {
                                      timestamp <- now
                                      entry      = Entry(
                                        value = value,
                                        release = {
                                          val result = for {
                                            result <- release.attempt
                                            result <- ref
                                              .modify {
                                                case state: State.Allocated =>
                                                  (
                                                    state.copy(releasing = state.releasing - id),
                                                    ().pure[F]
                                                  )

                                                case state: State.Released =>
                                                  val releasing = state.releasing - id
                                                  (
                                                    state.copy(releasing = releasing),
                                                    result match {
                                                      case Right(a) =>
                                                        if (releasing.isEmpty && state.allocated.isEmpty) {
                                                          // this was the last resource in a pool,
                                                          // we can release the pool itself now
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
                                                  )
                                              }
                                              .flatten
                                              .uncancelable
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
                                    // resource failed to allocate
                                    ref
                                      .modify {
                                        case state: State.Allocated =>

                                          val entries = state.entries - id

                                          def stateOf(stage: State.Allocated.Stage) = {
                                            state.copy(
                                              entries = entries,
                                              stage = stage)
                                          }

                                          if (entries.isEmpty) {
                                            state.stage match {
                                              case stage: State.Allocated.Stage.Free =>
                                                (
                                                  stateOf(stage),
                                                  ().pure[F]
                                                )
                                              case stage: State.Allocated.Stage.Busy =>
                                                (
                                                  stateOf(State.Allocated.Stage.free(List.empty)),
                                                  stage
                                                    .tasks
                                                    .foldMapM { task =>
                                                      task
                                                        .complete(a.asLeft)
                                                        .void
                                                    }
                                                )
                                            }
                                          } else {
                                            (
                                              stateOf(stage),
                                              ().pure[F]
                                            )
                                          }

                                        case state: State.Released =>

                                          val allocated = state.allocated - id

                                          def stateOf(tasks: Tasks) = {
                                            state.copy(
                                              allocated = allocated,
                                              tasks = tasks)
                                          }

                                          if (allocated.isEmpty) {
                                            (
                                              stateOf(Queue.empty),
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
                                            )
                                          } else {
                                            (
                                              stateOf(state.tasks),
                                              ().pure[F]
                                            )
                                          }
                                      }
                                      .flatten
                                      .uncancelable
                                      .productR { a.raiseError[F, Result] }
                                }
                            }
                          } else {
                            // pool is already full, add a task into a waiting queue
                            enqueue(Queue.empty)
                          }
                      }

                    case stage: State.Allocated.Stage.Busy =>
                      enqueue(stage.tasks)
                  }

                case (_: State.Released, _) =>
                  ReleasedError.raiseError[F, Either[Int, Result]]
              }
          }
        }
      }
    }
  }

  def const[F[_]: MonadCancelThrow, A](value: Resource[F, A]): ResourcePool[F, A] = {
    const(value.allocated)
  }

  def const[F[_], A](value: F[(A, Release[F])]): ResourcePool[F, A] = {
    class Const
    new Const with ResourcePool[F, A] {
      def get = value
    }
  }

  final case object ReleasedError extends RuntimeException("released") with NoStackTrace

  final case class IllegalStateError(msg: String) extends RuntimeException(msg) with NoStackTrace


  implicit class ResourcePoolOps[F[_], A](val self: ResourcePool[F, A]) extends AnyVal {

    /** Returns a `Resource`, which, when allocated, will take a resource from a
      * pool.
      *
      * When the `Resource` is released then the underlying resource is released
      * back to the pool.
      */
    def resource(implicit F: Functor[F]): Resource[F, A] = Resource(self.get)
  }

  object implicits {
    implicit class ResourceOpsResourcePool[F[_], A](val self: Resource[F, A]) extends AnyVal {

      /** Same as [[of[F[_],A](maxSize:Int,expireAfter*]], but provides a
        * shorter syntax to create a pool out of existing resource.
        */
      def toResourcePool(
        maxSize: Int,
        expireAfter: FiniteDuration,
      )(implicit
        F: Async[F],
        P: Parallel[F]
      ): Resource[F, ResourcePool[F, A]] = {
        ResourcePool.of(maxSize, expireAfter, _ => self)
      }

      /** Same as [[of[F[_],A](maxSize:Int,partitions:Int*]], but provides a
        * shorter syntax to create a pool out of existing resource.
        */
      def toResourcePool(
        maxSize: Int,
        partitions: Int,
        expireAfter: FiniteDuration,
      )(implicit
        F: Async[F],
        P: Parallel[F]
      ): Resource[F, ResourcePool[F, A]] = {
        ResourcePool.of(maxSize = maxSize, partitions = partitions, expireAfter, _ => self)
      }
    }
  }
}
