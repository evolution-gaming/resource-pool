package com.evolution.resourcepool

import cats.Functor
import cats.effect.*
import cats.effect.kernel.Resource.ExitCase
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.evolution.resourcepool.IntHelper.*

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

trait ResourcePool[F[_], A] {
  import ResourcePool.Release

  /** Returns the acquired resource, and a handle releasing it back to the pool.
    *
    * Calling the handle will not release resource itself, but just make it available to be returned again, though
    * resource may expire and be released if it stays unused for long enough.
    *
    * The resource leak may occur if the release handle is never called. Therefore it is recommended to use
    * [[ResourcePool.ResourcePoolOps#resource]] method instead, which will return [[cats.effect.Resource]] calling the
    * handle on release.
    */
  def get: F[(A, Release[F])]
}

object ResourcePool {

  type Release[F[_]] = F[Unit]

  type Id = String

  /** Same as [[of[F[_], A](maxSize: Int, partitions: Int, ...)]], but number of partitions is determined automatically
    * by taking into account the number of available processors and expected pool size.
    */
  def of[F[_]: Async, A](
    maxSize: Int,
    expireAfter: FiniteDuration,
    discardTasksOnRelease: Boolean,
    resource: Id => Resource[F, A],
  ): Resource[F, ResourcePool[F, A]] = {

    def apply(maxSize: Int): Resource[F, ResourcePool[F, A]] =
      for {
        cpus <- Sync[F].delay(Runtime.getRuntime.availableProcessors()).toResource
        result <- of(
          maxSize = maxSize,
          partitions = (maxSize / 100).min(cpus),
          expireAfter,
          discardTasksOnRelease,
          resource,
        )
      } yield result

    apply(maxSize.max(1))
  }

  /** Creates a new pool with specified number of partitions.
    *
    * @param maxSize
    *   Maximum size of the whole pool.
    * @param partitions
    *   Number of partitions to be used. This number determines the count of the threads, that could access the pool in
    *   parallel, and also number of background processes removing the expiring entries.
    * @param expireAfter
    *   Duration after which the resource should be removed if unused.
    * @param resource
    *   Factory for creating the new resources. `Id` is a unique identifier of a resource that could be used, for
    *   example, for logging purposes.
    */
  def of[F[_]: Async, A](
    maxSize: Int,
    partitions: Int,
    expireAfter: FiniteDuration,
    discardTasksOnRelease: Boolean,
    resource: Id => Resource[F, A],
  ): Resource[F, ResourcePool[F, A]] = {

    def apply(maxSize: Int, partitions: Int): Resource[F, ResourcePool[F, A]] = {

      def of(maxSize: Int)(resource: Id => Resource[F, A]): Resource[F, ResourcePool[F, A]] =
        of0(maxSize, expireAfter, discardTasksOnRelease, resource)

      if (partitions <= 1)
        of(maxSize)(resource)
      else
        for {
          ref <- Ref[F].of(0).toResource
          values0 <- maxSize.divide(partitions).zipWithIndex.parTraverse { case (maxSize, idx) =>
            of(maxSize)(id => resource(s"$id-$idx"))
          }
          values = values0.toVector
          length = values.length
        } yield new ResourcePool[F, A] {
          def get: F[(A, Release[F])] =
            MonadCancel[F].uncancelable { poll =>
              ref
                .modify { a =>
                  val b = a + 1
                  (
                    if (b < length) b else 0,
                    a,
                  )
                }
                .flatMap { partition =>
                  poll(values.apply(partition).get)
                }
            }
        }
    }

    apply(maxSize = maxSize.max(1), partitions = partitions.max(1))
  }

  private def of0[F[_]: Async, A](
    maxSize: Int,
    expireAfter: FiniteDuration,
    discardTasksOnRelease: Boolean,
    resource: Id => Resource[F, A],
  ): Resource[F, ResourcePool[F, A]] = {

    type Id = Long
    type Ids = List[Id]
    type Release = ResourcePool.Release[F]
    type Result = (A, Release)
    type Task = Deferred[F, Either[Throwable, (Id, Entry)]]
    type Tasks = Queue[Task]

    def now: F[FiniteDuration] = Temporal[F].realTime

    final case class Entry(value: A, release: F[Unit], timestamp: FiniteDuration) {
      def renew: F[Entry] = now.map(now => copy(timestamp = now))
    }

    sealed trait State

    object State {

      def empty: State =
        Allocated(
          id = 0L,
          entries = Map.empty,
          stage = Allocated.Stage.free(List.empty),
          releasing = Set.empty,
        )

      /** Resource pool is allocated.
        *
        * @param id
        *   Sequence number of a last resource allocated (used to generate an identifier for a next resource).
        * @param entries
        *   Allocated or allocating resources. `Some` means that resource is allocated, and `None` means allocation is
        *   in progress.
        * @param stage
        *   Represents a state of a pool, i.e. if it is fully busy, if there are free resources, and the tasks waiting
        *   for resources to be freed.
        * @param releasing
        *   List of ids being released because these have expired.
        */
      final case class Allocated(
        id: Long,
        entries: Map[Id, Option[Entry]],
        stage: Allocated.Stage,
        releasing: Set[Id],
      ) extends State

      object Allocated {

        sealed trait Stage

        object Stage {

          def free(ids: Ids): Stage = Free(ids)

          def busy(tasks: Tasks): Stage = Busy(tasks)

          /** There are free resources to use.
            *
            * @param ids
            *   List of ids from [[Allocated#Entries]] that are free to use. It could be equal to `Nil` if all resources
            *   are busy, but there are no tasks waiting in queue.
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
        *   List of ids being released (either because pool is releasing or because these expired earlier).
        * @param tasks
        *   The list of tasks left to be completed before the pool could be released.
        * @param released
        *   `Deferred`, which will be completed, when all the tasks are completed and all resources are released.
        */
      final case class Released(
        allocated: Set[Id],
        releasing: Set[Id],
        tasks: Tasks,
        released: Deferred[F, Either[Throwable, Unit]],
      ) extends State
    }

    for {
      ref <- Resource.make {
        Ref[F].of(State.empty)
      } { ref =>
        0.tailRecM { count =>
          ref.access.flatMap {
            case (state: State.Allocated, set) =>
              Deferred.apply[F, Either[Throwable, Unit]].flatMap { released =>
                def apply(allocated: Set[Id], releasing: Set[Id], tasks: Tasks)(
                  effect: => F[Unit],
                ): F[Either[Int, Unit]] =
                  set
                    .apply(State.Released(allocated = allocated, releasing = releasing, tasks, released))
                    .flatMap {
                      case true =>
                        for {
                          result <-
                            if (allocated.isEmpty && releasing.isEmpty)
                              // the pool is empty now, we can safely release it
                              released.complete(().asRight).void
                            else
                              // the pool will be released elsewhere when all resources in `allocated` or
                              // `releasing` get released
                              effect.productR(released.get.rethrow)
                        } yield result.asRight[Int]
                      case false =>
                        (count + 1).asLeft[Unit].pure[F]
                    }
                    .uncancelable

                state.stage match {
                  case stage: State.Allocated.Stage.Free =>
                    // glue `release` functions of all free resources together
                    val (entries, releasing, release) =
                      stage.ids.foldLeft((state.entries, state.releasing, ().pure[F])) {
                        case ((entries, releasing, release), id) =>
                          entries.get(id).fold((entries, releasing, release)) {
                            case Some(entry) => (entries - id, releasing + id, release.productR(entry.release))
                            case None => (entries, releasing, release)
                          }
                      }

                    apply(
                      allocated = entries.keySet,
                      releasing = releasing,
                      tasks = Queue.empty,
                    )(release)

                  case stage: State.Allocated.Stage.Busy =>
                    if (discardTasksOnRelease)
                      apply(
                        allocated = state.entries.keySet,
                        releasing = state.releasing,
                        tasks = Queue.empty,
                      )(stage.tasks.foldMapM(_.complete(ReleasedError.asLeft).void))
                    else
                      apply(
                        allocated = state.entries.keySet,
                        releasing = state.releasing,
                        tasks = stage.tasks,
                      )(().pure[F])
                }
              }

            case (state: State.Released, _) =>
              state.released.get.rethrow.map(_.asRight[Int])
          }
        }
      }
      _ <- Async[F].background {
        val interval = expireAfter / 10
        for {
          _ <- Temporal[F].sleep(expireAfter)
          a <- Async[F].foreverM[Unit, Unit] {
            for {
              now <- now
              threshold = now - expireAfter
              result <- 0.tailRecM { count =>
                ref.access.flatMap {
                  case (state: State.Allocated, set) =>
                    state.stage match {
                      case stage: State.Allocated.Stage.Free =>
                        val (ids, entries, releasing, release) =
                          stage.ids.foldLeft((List.empty[Id], state.entries, state.releasing, ().pure[F])) {
                            case ((ids, entries, releasing, release), id) =>
                              entries.get(id).fold((ids, entries, releasing, release)) {
                                case Some(entry) if entry.timestamp < threshold =>
                                  (ids, entries - id, releasing + id, release.productR(entry.release))
                                case Some(_) =>
                                  (id :: ids, entries, releasing, release)
                                case None =>
                                  (ids, entries, releasing, release)
                              }
                          }

                        set
                          .apply {
                            state.copy(
                              entries = entries,
                              stage = stage.copy(ids = ids.reverse),
                              releasing = releasing,
                            )
                          }
                          .flatMap {
                            case true =>
                              release.map(_.asRight[Int])
                            case false =>
                              (count + 1).asLeft[Unit].pure[F]
                          }
                          .uncancelable

                      case _: State.Allocated.Stage.Busy =>
                        ().asRight[Int].pure[F]
                    }

                  case (_: State.Released, _) =>
                    ().asRight[Int].pure[F]
                }
              }
              _ <- Temporal[F].sleep(interval)
            } yield result
          }
        } yield a
      }
    } yield {
      new ResourcePool[F, A] {
        def get: F[(A, ResourcePool.Release[F])] = {

          def entryAdd(id: Id, entry: Entry): F[Unit] =
            0.tailRecM { count =>
              ref.access.flatMap {
                case (state: State.Allocated, set) =>
                  set
                    .apply(state.copy(entries = state.entries.updated(id, entry.some)))
                    .map {
                      case true => ().asRight[Int]
                      case false => (count + 1).asLeft[Unit]
                    }
                case (_: State.Released, _) =>
                  ().asRight[Int].pure[F]
              }
            }

          def entryRemove(id: Id, error: Throwable): F[Unit] =
            ref.modify {
              case state: State.Allocated =>
                val entries = state.entries - id

                def stateOf(stage: State.Allocated.Stage): State.Allocated =
                  state.copy(entries = entries, stage = stage)

                if (entries.isEmpty)
                  state.stage match {
                    case stage: State.Allocated.Stage.Free =>
                      val nextState = stateOf(stage)
                      val result = ().pure[F]
                      (nextState, result)
                    case stage: State.Allocated.Stage.Busy =>
                      val nextState = stateOf(State.Allocated.Stage.free(List.empty))
                      val result = stage.tasks.foldMapM(_.complete(error.asLeft).void)
                      (nextState, result)
                  }
                else {
                  val nextState = stateOf(state.stage)
                  val result = ().pure[F]
                  (nextState, result)
                }

              case state: State.Released =>
                val allocated = state.allocated - id

                def stateOf(tasks: Tasks): State.Released =
                  state.copy(allocated = allocated, tasks = tasks)

                if (allocated.isEmpty) {
                  val nextState = stateOf(Queue.empty)
                  val result = state.tasks
                    .foldMapM(_.complete(error.asLeft).void)
                    .productR {
                      if (state.releasing.isEmpty) state.released.complete(().asRight).void
                      else ().pure[F]
                    }
                  (nextState, result)
                } else {
                  val nextState = stateOf(state.tasks)
                  val result = ().pure[F]
                  (nextState, result)
                }
            }.flatten

          def entryRelease(id: Id, release: Release): F[Unit] =
            for {
              release <- release.attempt
              result <- ref.modify {
                case state: State.Allocated =>
                  val nextState = state.copy(releasing = state.releasing - id)
                  val result = ().pure[F]
                  (nextState, result)

                case state: State.Released =>
                  val releasing = state.releasing - id
                  val nextState = state.copy(releasing = releasing)
                  val result = release match {
                    case Right(a) =>
                      if (releasing.isEmpty && state.allocated.isEmpty)
                        // this was the last resource in a pool,
                        // we can release the pool itself now
                        state.released.complete(a.asRight).void
                      else
                        ().pure[F]
                    case Left(error) =>
                      state.released.complete(error.asLeft).void
                  }
                  (nextState, result)

              }.flatten
            } yield result

          def releaseOf(id: Id, entry: Entry): Release =
            for {
              entry <- entry.renew
              result <- ref
                .modify {
                  case state: State.Allocated =>
                    def stateOf(stage: State.Allocated.Stage): State.Allocated =
                      state.copy(entries = state.entries.updated(id, entry.some), stage = stage)

                    state.stage match {
                      case stage: State.Allocated.Stage.Free =>
                        val nextState = stateOf(stage.copy(ids = id :: stage.ids))
                        val result = ().pure[F]
                        (nextState, result)
                      case stage: State.Allocated.Stage.Busy =>
                        stage.tasks.dequeueOption
                          .fold {
                            val nextState = stateOf(State.Allocated.Stage.free(List(id)))
                            val result = ().pure[F]
                            (nextState, result)
                          } { case (task, tasks) =>
                            val nextState = stateOf(stage.copy(tasks = tasks))
                            val result = task.complete((id, entry).asRight).void
                            (nextState, result)
                          }
                    }

                  case state: State.Released =>
                    state.tasks.dequeueOption
                      .fold {
                        val nextState = state.copy(allocated = state.allocated - id, releasing = state.releasing + id)
                        val result = entry.release
                        (nextState, result)
                      } { case (task, tasks) =>
                        val nextState = state.copy(tasks = tasks)
                        val result = task.complete((id, entry).asRight).void
                        (nextState, result)
                      }
                }
                .flatten
                .uncancelable
            } yield result

          def removeTask(task: Task): F[Unit] =
            ref.update {
              case state: State.Allocated =>
                state.stage match {
                  case _: State.Allocated.Stage.Free =>
                    state
                  case stage: State.Allocated.Stage.Busy =>
                    state.copy(
                      stage = stage.copy(
                        tasks = stage.tasks.filter(_ ne task),
                      ),
                    )
                }

              case state: State.Released =>
                state.copy(
                  tasks = state.tasks.filter(_ ne task),
                )
            }

          MonadCancel[F].uncancelable { poll =>
            0.tailRecM { count =>
              ref.access.flatMap {
                case (state: State.Allocated, set) =>
                  def apply[X](state: State.Allocated)(effect: => F[X]): F[Either[Int, X]] =
                    set.apply(state).flatMap {
                      case true =>
                        effect.map(_.asRight[Int])
                      case false =>
                        (count + 1).asLeft[X].pure[F]
                    }

                  def enqueue(tasks: Tasks): F[Either[Int, (A, Release)]] =
                    Deferred.apply[F, Either[Throwable, (Id, Entry)]].flatMap { task =>
                      apply(state.copy(stage = State.Allocated.Stage.busy(tasks.enqueue(task)))) {
                        poll
                          .apply(task.get)
                          .onCancel(removeTask(task))
                          .rethrow
                          .map { case (id, entry) =>
                            (entry.value, releaseOf(id, entry))
                          }
                      }
                    }

                  state.stage match {
                    case stage: State.Allocated.Stage.Free =>
                      stage.ids match {
                        // there are free resources to use
                        case id :: ids =>
                          state.entries
                            .get(id)
                            .fold {
                              IllegalStateError(s"entry is not found, id: $id")
                                .raiseError[F, Either[Int, (A, Release)]]
                            } { entry =>
                              entry.fold {
                                IllegalStateError(s"entry is not defined, id: $id")
                                  .raiseError[F, Either[Int, (A, Release)]]
                              } { entry =>
                                entry.renew
                                  .flatMap { entry =>
                                    apply {
                                      state.copy(
                                        stage = stage.copy(ids),
                                        entries = state.entries.updated(id, entry.some),
                                      )
                                    } {
                                      (entry.value, releaseOf(id, entry)).pure[F]
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
                            apply(state.copy(id = id + 1, entries = state.entries.updated(id, none))) {
                              poll
                                .apply(resource.apply(id.toString).allocated)
                                .onCancel(entryRemove(id, CancelledError))
                                .attempt
                                .flatMap {
                                  case Right((value, release)) =>
                                    // resource was allocated
                                    for {
                                      now <- now
                                      entry = Entry(
                                        value = value,
                                        release = entryRelease(id, release).start.void,
                                        timestamp = now,
                                      )
                                      _ <- entryAdd(id, entry)
                                    } yield (value, releaseOf(id, entry))
                                  case Left(a) =>
                                    // resource failed to allocate
                                    entryRemove(id, a).productR(a.raiseError[F, Result])
                                }
                            }
                          } else
                            // pool is already full, add a task into a waiting queue
                            enqueue(Queue.empty)
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
  }

  def const[F[_]: MonadCancelThrow, A](value: Resource[F, A]): ResourcePool[F, A] =
    const(value.allocated)

  def const[F[_], A](value: F[(A, Release[F])]): ResourcePool[F, A] = {
    class Const
    new Const with ResourcePool[F, A] {
      def get: F[(A, Release[F])] = value
    }
  }

  final case object ReleasedError extends RuntimeException("released") with NoStackTrace

  final case object CancelledError extends RuntimeException("cancelled") with NoStackTrace

  final case class IllegalStateError(msg: String) extends RuntimeException(msg) with NoStackTrace

  implicit class ResourcePoolOps[F[_], A](val self: ResourcePool[F, A]) extends AnyVal {

    /** Returns a `Resource`, which, when allocated, will take a resource from a pool.
      *
      * When the `Resource` is released then the underlying resource is released back to the pool.
      */
    def resource(implicit F: Functor[F]): Resource[F, A] =
      Resource.applyFull {
        _.apply(self.get).map { case (a, release) => (a, (_: ExitCase) => release) }
      }
  }

  object implicits {
    implicit class ResourceOpsResourcePool[F[_], A](val self: Resource[F, A]) extends AnyVal {

      /** Same as [[of[F[_], A](maxSize: Int, expireAfter ...]], but provides a shorter syntax to create a pool out of
        * existing resource.
        */
      def toResourcePool(
        maxSize: Int,
        expireAfter: FiniteDuration,
      )(implicit
        F: Async[F],
      ): Resource[F, ResourcePool[F, A]] =
        toResourcePool(maxSize, expireAfter, discardTasksOnRelease = false)

      /** Same as [[of[F[_], A](maxSize: Int, expireAfter ...]], but provides a shorter syntax to create a pool out of
        * existing resource.
        */
      def toResourcePool(
        maxSize: Int,
        expireAfter: FiniteDuration,
        discardTasksOnRelease: Boolean,
      )(implicit
        F: Async[F],
      ): Resource[F, ResourcePool[F, A]] =
        ResourcePool.of(maxSize, expireAfter, discardTasksOnRelease, _ => self)

      /** Same as [[of[F[_], A](maxSize: Int, partitions: Int, ...]], but provides a shorter syntax to create a pool out
        * of existing resource.
        */
      def toResourcePool(
        maxSize: Int,
        partitions: Int,
        expireAfter: FiniteDuration,
        discardTasksOnRelease: Boolean,
      )(implicit
        F: Async[F],
      ): Resource[F, ResourcePool[F, A]] =
        ResourcePool.of(maxSize, partitions, expireAfter, discardTasksOnRelease, _ => self)
    }
  }
}
