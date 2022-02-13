package fs2.grpc.server.internal

import cats.effect._
import cats.syntax.functor._
import fs2.grpc.server.internal.OneShotChannel.State
import scala.annotation.nowarn
import scala.collection.immutable.Queue

@nowarn
final class OneShotChannel[A](val ref: Ref[SyncIO, State[A]]) extends AnyVal {

  import State._

  /** Send message to stream.
    */
  def send(a: A): SyncIO[Unit] =
    ref
      .modify {
        case open: Open[A] => (open.append(a), SyncIO.unit)
        case s: Suspended[A] => (Consumed, s.resume(new Open(Queue(a))))
        case closed => (closed, SyncIO.unit)
      }.flatMap(identity)

  /** Close stream.
    */
  def close(): SyncIO[Unit] =
    ref
      .modify {
        case open: Open[A] => (open.close(), SyncIO.unit)
        case s: Suspended[A] => (Done, s.resume(Done))
        case closed => (closed, SyncIO.unit)
      }.flatMap(identity)

  import fs2._

  /** This method can be called at most once
    */
  def stream[F[_]](implicit F: Async[F]): Stream[F, A] = {
    def go(): Pull[F, A, Unit] =
      Pull
        .eval(ref.getAndSet(Consumed).to[F])
        .flatMap {
          case Consumed =>
            Pull.eval(F.async[State[A]] { cb =>
              val next = new Suspended[A](s => cb(Right(s)))
              ref
                .modify {
                  case Consumed => (next, None)
                  case other => (Consumed, Some(other))
                }
                .to[F]
                .map {
                  case Some(received) =>
                    cb(Right(received))
                    None
                  case None =>
                    Some(ref.set(Done).to[F])
                }
            })
          case other => Pull.pure(other)
        }
        .flatMap {
          case open: Open[A] => Pull.output(Chunk.queue(open.queue)) >> go()
          case completed: Closed[A] => Pull.output(Chunk.queue(completed.queue))
          case _: Suspended[A] => Pull.done // unexpected
        }

    go().stream
  }
}

object OneShotChannel {
  def empty[A]: SyncIO[OneShotChannel[A]] =
    Ref[SyncIO].of[State[A]](State.Consumed).map(new OneShotChannel[A](_))

  sealed trait State[+A]

  object State {
    private[OneShotChannel] val Consumed: State[Nothing] = new Open(Queue.empty)
    private[OneShotChannel] val Cancelled: State[Nothing] = new Closed(Queue.empty)
    private[OneShotChannel] val Done: State[Nothing] = new Closed(Queue.empty)

    class Open[A](val queue: Queue[A]) extends State[A] {
      def append(a: A): Open[A] = new Open(queue.enqueue(a))

      def close(): Closed[A] = new Closed(queue)
    }

    class Closed[A](val queue: Queue[A]) extends State[A]

    class Suspended[A](f: State[A] => Unit) extends State[A] {
      def resume(state: State[A]): SyncIO[Unit] = SyncIO(f(state))
    }
  }
}