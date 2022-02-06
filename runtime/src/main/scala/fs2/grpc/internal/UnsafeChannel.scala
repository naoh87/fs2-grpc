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

import cats.effect._
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicBoolean
import UnsafeChannel.State
import scala.collection.immutable.Queue

final class UnsafeChannel[A] extends AtomicReference[State[A]](State.Consumed) {

  import State._
  import scala.annotation._

  /** Send message to stream.
    */
  @nowarn
  @tailrec
  def send(a: A): Unit = {
    get() match {
      case open: Open[A] =>
        if (!compareAndSet(open, open.append(a))) {
          send(a)
        }
      case s: Suspended[A] =>
        lazySet(Consumed)
        s.resume(new Open(Queue(a)))
      case closed: Closed[A] =>
    }
  }

  /** Close stream.
    */
  @tailrec
  def close(): Unit =
    get() match {
      case open: Open[_] =>
        if (!compareAndSet(open, open.close())) {
          close()
        }
      case s: Suspended[_] =>
        lazySet(Done)
        s.resume(Done)
      case _ =>
    }

  import fs2._

  /** This method can be called at most once
    */
  def stream[F[_]](implicit F: Async[F]): Stream[F, A] = {
    @nowarn
    def go(): Pull[F, A, Unit] =
      Pull
        .suspend {
          val got = getAndSet(Consumed)
          if (got eq Consumed) {
            Pull.eval(F.async[State[A]] { cb =>
              F.delay {
                val next = new Suspended[A](s => cb(Right(s)))
                if (!compareAndSet(Consumed, next)) {
                  cb(Right(getAndSet(Consumed)))
                  None
                } else {
                  Some(F.delay(cb(Right(Cancelled))))
                }
              }
            })
          } else Pull.pure(got)
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
  def empty[A]: UnsafeChannel[A] = new UnsafeChannel[A]

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
    }
  }
}
