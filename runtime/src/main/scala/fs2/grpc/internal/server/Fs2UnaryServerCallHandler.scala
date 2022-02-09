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

  import Fs2ServerCall.Cancel

  sealed trait Context[A]
  object Context {
    def init[A](cb: A => SyncIO[Cancel]): SyncIO[Ref[SyncIO, Context[A]]] =
      Ref[SyncIO].of[Context[A]](BeforeCall(cb, None))
  }
  case class BeforeCall[A](cb: A => SyncIO[Cancel], request: Option[A]) extends Context[A] {
    def isEmpty: Boolean = request.isEmpty
    def set(a: A): BeforeCall[A] = copy(request = Some(a))
  }
  case class Called[A](cancel: Cancel) extends Context[A]

  private def mkListener[Request, Response](
      call: Fs2ServerCall[Request, Response],
      ctx: Ref[SyncIO, Context[Request]]
  ): ServerCall.Listener[Request] =
    new ServerCall.Listener[Request] {
      override def onCancel(): Unit =
        ctx.get
          .flatMap {
            case Called(cancel) => cancel
            case _ => SyncIO.unit
          }
          .unsafeRunSync()

      override def onMessage(message: Request): Unit =
        ctx.get
          .flatMap {
            case p: BeforeCall[Request] if p.isEmpty => ctx.set(p.set(message))
            case c => sendError(c, Status.INTERNAL.withDescription("Too many requests"))
          }
          .unsafeRunSync()

      override def onHalfClose(): Unit =
        ctx.get
          .flatMap {
            case BeforeCall(cb, Some(r)) => cb(r).flatMap(c => ctx.set(Called(c)))
            case c => sendError(c, Status.INTERNAL.withDescription("Half-closed without a request"))
          }
          .unsafeRunSync()

      private def sendError(c: Context[Request], state: Status): SyncIO[Unit] =
        c match {
          case _: BeforeCall[Request] => ctx.set(Called(SyncIO.unit)) >> call.close(state, new Metadata())
          case _ => SyncIO.unit
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
  )(f: Fs2ServerCall[Request, Response] => Request => SyncIO[Cancel]): SyncIO[ServerCall.Listener[Request]] = {
    for {
      call <- Fs2ServerCall.setup(options, call)
      _ <- call.request(2)
      ctx <- Context.init(f(call))
    } yield mkListener[Request, Response](call, ctx)
  }
}
