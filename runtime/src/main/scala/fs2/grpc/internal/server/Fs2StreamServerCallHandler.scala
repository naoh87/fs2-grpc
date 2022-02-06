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
import cats.effect.SyncIO
import cats.effect.std.Dispatcher
import io.grpc._
import fs2._
import fs2.grpc.internal.UnsafeChannel
import fs2.grpc.server.ServerOptions
import fs2.grpc.server.ServerCallOptions
import io.grpc.ServerCallHandler

object Fs2StreamServerCallHandler {

  import Fs2StatefulServerCall.Cancel

  private def mkListener[F[_]: Async, Request, Response](
      ch: UnsafeChannel[Request],
      run: Stream[F, Request] => SyncIO[Cancel],
      call: ServerCall[Request, Response]
  ): ServerCall.Listener[Request] =
    new ServerCall.Listener[Request] {
      private[this] val cancel: Cancel = run(ch.stream.mapChunks { chunk =>
        val size = chunk.size
        if (size > 0) call.request(size)
        chunk
      }).unsafeRunSync()

      override def onCancel(): Unit =
        cancel.unsafeRunSync()

      override def onMessage(message: Request): Unit =
        ch.send(message).unsafeRunSync()

      override def onHalfClose(): Unit =
        ch.close().unsafeRunSync()
    }

  def unary[F[_]: Async, Request, Response](
      impl: (Stream[F, Request], Metadata) => F[Response],
      options: ServerOptions,
      dispatcher: Dispatcher[F]
  ): ServerCallHandler[Request, Response] =
    new ServerCallHandler[Request, Response] {
      private val opt = options.callOptionsFn(ServerCallOptions.default)

      def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] = {
        for {
          responder <- Fs2StatefulServerCall.setup(opt, call)
          _ <- responder.request(1)
          ch <- UnsafeChannel.empty[Request]
        } yield mkListener[F, Request, Response](ch, req => responder.unary(impl(req, headers), dispatcher), call)
      }.unsafeRunSync()
    }

  def stream[F[_]: Async, Request, Response](
      impl: (Stream[F, Request], Metadata) => Stream[F, Response],
      options: ServerOptions,
      dispatcher: Dispatcher[F]
  ): ServerCallHandler[Request, Response] =
    new ServerCallHandler[Request, Response] {
      private val opt = options.callOptionsFn(ServerCallOptions.default)

      def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] = {
        for {
          responder <- Fs2StatefulServerCall.setup(opt, call)
          _ <- responder.request(1)
          ch <- UnsafeChannel.empty[Request]
        } yield mkListener[F, Request, Response](ch, req => responder.stream(impl(req, headers), dispatcher), call)
      }.unsafeRunSync()
    }
}
