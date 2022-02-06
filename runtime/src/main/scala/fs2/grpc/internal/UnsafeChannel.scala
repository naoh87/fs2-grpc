/*
 * Copyright (c) 2018 Gary Coady / Fs2 Grpc Developers
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2.grpc.internal

import java.util.concurrent.atomic.AtomicBoolean
import cats.effect._
import fs2.grpc.internal.UnsafeChannel.State
import scala.annotation.nowarn
import scala.collection.immutable.Queue

@nowarn
final class UnsafeChannel[A](val ref: Ref[SyncIO, State[A]]) extends AnyVal {

  import State._

  /** Send message to stream.
    */
  def send(a: A): SyncIO[Unit] =
    ref
      .modify[SyncIO[Unit]] {
        case open: Open[A] =>
          (open.append(a), SyncIO.unit)
        case s: Suspended[A] =>
          (Consumed, s.resumeS(new Open(Queue(a))))
        case closed =>
          (closed, SyncIO.unit)
      }
      .flatMap(identity)

  /** Close stream.
    */
  def close(): SyncIO[Unit] =
    ref
      .modify {
        case open: Open[A] =>
          (open.close(), SyncIO.unit)
        case s: Suspended[A] =>
          (Done, s.resumeS(Done))
        case closed =>
          (closed, SyncIO.unit)
      }
      .flatMap(identity)

  import fs2._

  /** This method can be called at most once
    */
  def stream[F[_]](implicit F: Async[F]): Stream[F, A] = {
    def go(): Pull[F, A, Unit] =
      Pull
        .eval(ref.get.to[F])
        .flatMap {
          case Consumed =>
            Pull.eval(F.async[State[A]] { cb =>
              val next = new Suspended[A](s => cb(Right(s)))
              ref
                .modify {
                  case Consumed => (next, None)
                  case other => (Consumed, Some(other))
                }
                .map {
                  case None =>
                    Some(F.delay(cb(Right(Cancelled))))
                  case Some(value) =>
                    cb(Right(value))
                    None
                }
                .to[F]
            })
          case other => Pull.pure(other)
        }
        .flatMap {
          case open: Open[A] => Pull.output(Chunk.queue(open.queue)) >> go()
          case completed: Closed[A] => Pull.output(Chunk.queue(completed.queue))
          case suspended: Suspended[A] => Pull.done // unexpected
        }

    go().stream
  }
}

object UnsafeChannel {
  def empty[A]: SyncIO[UnsafeChannel[A]] =
    Ref[SyncIO].of[State[A]](State.Consumed).map(new UnsafeChannel[A](_))

  sealed trait State[+A]

  object State {
    private[UnsafeChannel] val Consumed: State[Nothing] = new Open(Queue.empty)
    private[UnsafeChannel] val Cancelled: State[Nothing] = new Closed(Queue.empty)
    private[UnsafeChannel] val Done: State[Nothing] = new Closed(Queue.empty)

    class Open[A](val queue: Queue[A]) extends State[A] {
      def append(a: A): Open[A] = new Open(queue.enqueue(a))

      def close(): Closed[A] = new Closed(queue)
    }

    class Closed[A](val queue: Queue[A]) extends State[A]

    class Suspended[A](val f: State[A] => Unit) extends AtomicBoolean(false) with State[A] {
      def resume(state: State[A]): Unit =
        if (!getAndSet(true)) {
          f(state)
        }

      def resumeS(state: State[A]): SyncIO[Unit] =
        SyncIO(if (!getAndSet(true)) {
          f(state)
        })
    }
  }
}
