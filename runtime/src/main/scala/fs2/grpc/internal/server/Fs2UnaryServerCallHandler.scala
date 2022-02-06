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

import cats.effect.Async
import cats.effect.Ref
import cats.effect.SyncIO
import cats.effect.std.Dispatcher
import fs2.grpc.server.ServerCallOptions
import fs2.grpc.server.ServerOptions
import io.grpc._

object Fs2UnaryServerCallHandler {

  import Fs2StatefulServerCall.Cancel

  case class State[Request](cancel: Option[Cancel], request: Option[Request])

  private def mkListener[Request, Response](
      run: Request => SyncIO[Cancel],
      call: Fs2StatefulServerCall[Request, Response],
      state: Ref[SyncIO, State[Request]]
  ): ServerCall.Listener[Request] =
    new ServerCall.Listener[Request] {
      override def onCancel(): Unit =
        state.get.flatMap(_.cancel.getOrElse(SyncIO.unit)).unsafeRunSync()

      override def onMessage(message: Request): Unit =
        state.get
          .flatMap {
            case cur if cur.request.isEmpty =>
              state.set(cur.copy(request = Some(message)))
            case cur =>
              earlyClose(cur, Status.INTERNAL.withDescription("Too many requests"))
          }
          .unsafeRunSync()

      override def onHalfClose(): Unit =
        state.get
          .flatMap {
            case State(None, Some(request)) =>
              run(request).flatMap(c => state.set(State(Some(c), None)))
            case cur =>
              earlyClose(cur, Status.INTERNAL.withDescription("Half-closed without a request"))
          }
          .unsafeRunSync()

      private def earlyClose(current: State[Request], status: Status): SyncIO[Unit] = {
        if (current.cancel.isEmpty) {
          state.set(State(Some(SyncIO.unit), None)) >> call.close(status, new Metadata())
        } else {
          SyncIO.unit
        }
      }
    }

  def unary[F[_]: Async, Request, Response](
      impl: (Request, Metadata) => F[Response],
      options: ServerOptions,
      dispatcher: Dispatcher[F]
  ): ServerCallHandler[Request, Response] =
    new ServerCallHandler[Request, Response] {
      private val opt = options.callOptionsFn(ServerCallOptions.default)

      def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] =
        startCallSync(call, opt)(call => req => call.unary(impl(req, headers), dispatcher)).unsafeRunSync()
    }

  def stream[F[_]: Async, Request, Response](
      impl: (Request, Metadata) => fs2.Stream[F, Response],
      options: ServerOptions,
      dispatcher: Dispatcher[F]
  ): ServerCallHandler[Request, Response] =
    new ServerCallHandler[Request, Response] {
      private val opt = options.callOptionsFn(ServerCallOptions.default)

      def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] =
        startCallSync(call, opt)(call => req => call.stream(impl(req, headers), dispatcher)).unsafeRunSync()
    }

  private def startCallSync[F[_], Request, Response](
      call: ServerCall[Request, Response],
      options: ServerCallOptions
  )(f: Fs2StatefulServerCall[Request, Response] => Request => SyncIO[Cancel]): SyncIO[ServerCall.Listener[Request]] = {
    for {
      call <- Fs2StatefulServerCall.setup(options, call)
      _ <- call.request(2)
      state <- Ref.of[SyncIO, State[Request]](State(None, None))
    } yield mkListener[Request, Response](f(call), call, state)
  }
}
