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

package fs2.grpc.internal.server

import cats.effect._
import cats.effect.std.Dispatcher
import fs2.grpc.server.ServerCallOptions
import io.grpc._
import fs2._

object Fs2ServerCall {
  type Cancel = SyncIO[Unit]

  def setup[I, O](
      options: ServerCallOptions,
      call: ServerCall[I, O]
  ): SyncIO[Fs2ServerCall[I, O]] =
    SyncIO {
      call.setMessageCompression(options.messageCompression)
      options.compressor.map(_.name).foreach(call.setCompression)
      new Fs2ServerCall[I, O](call)
    }
}

final class Fs2ServerCall[Request, Response](
    call: ServerCall[Request, Response]
) {

  import Fs2ServerCall.Cancel

  def stream[F[_]](response: Stream[F, Response], dispatcher: Dispatcher[F])(implicit F: Async[F]): SyncIO[Cancel] =
    run(
      response.pull.peek1
        .flatMap {
          case Some((_, tail)) =>
            Pull.suspend {
              call.sendHeaders(new Metadata())
              tail.map(call.sendMessage).pull.echo
            }
          case None => Pull.done
        }
        .stream
        .compile
        .drain,
      dispatcher
    )

  def unary[F[_]](response: F[Response], dispatcher: Dispatcher[F])(implicit F: Async[F]): SyncIO[Cancel] =
    run(
      F.map(response) { message =>
        call.sendHeaders(new Metadata())
        call.sendMessage(message)
      },
      dispatcher
    )

  def request(n: Int): SyncIO[Unit] =
    SyncIO(call.request(n))

  def close(status: Status, metadata: Metadata): SyncIO[Unit] =
    SyncIO(call.close(status, metadata))

  private def run[F[_]](completed: F[Unit], dispatcher: Dispatcher[F])(implicit F: Sync[F]): SyncIO[Cancel] = {
    SyncIO {
      val cancel = dispatcher.unsafeRunCancelable(F.guaranteeCase(completed) {
        case Outcome.Succeeded(_) => closeStreamF(Status.OK, new Metadata())
        case Outcome.Errored(e) =>
          e match {
            case ex: StatusException =>
              closeStreamF(ex.getStatus, Option(ex.getTrailers).getOrElse(new Metadata()))
            case ex: StatusRuntimeException =>
              closeStreamF(ex.getStatus, Option(ex.getTrailers).getOrElse(new Metadata()))
            case ex =>
              closeStreamF(Status.INTERNAL.withDescription(ex.getMessage).withCause(ex), new Metadata())
          }
        case Outcome.Canceled() => closeStreamF(Status.CANCELLED, new Metadata())
      })
      SyncIO {
        cancel()
        ()
      }
    }
  }

  private def closeStreamF[F[_]](status: Status, metadata: Metadata)(implicit F: Sync[F]): F[Unit] =
    F.delay(call.close(status, metadata))
}
