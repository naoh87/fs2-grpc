package fs2.grpc.server.internal

import cats.effect.Async
import cats.effect.SyncIO
import cats.effect.std.Dispatcher
import fs2._
import fs2.grpc.server.ServerCallOptions
import fs2.grpc.server.ServerOptions
import fs2.grpc.server.internal.Fs2ServerCall.Cancel
import io.grpc.ServerCall
import io.grpc._

object Fs2StreamServerCallHandler {


  private def mkListener[Request](
    ch: OneShotChannel[Request],
    cancel: Cancel,
  ): ServerCall.Listener[Request] =
    new ServerCall.Listener[Request] {
      override def onCancel(): Unit =
        cancel.unsafeRunSync()

      override def onMessage(message: Request): Unit =
        ch.send(message).unsafeRunSync()

      override def onHalfClose(): Unit =
        ch.close().unsafeRunSync()
    }

  private def requestOnPull[F[_], A](call: ServerCall[A, _]): Pipe[F, A, A] =
    _.mapChunks { chunk =>
      call.request(chunk.size)
      chunk
    }

  private def mkHandler[F[_] : Async, Request, Response](
    start: (Stream[F, Request], Metadata) => Fs2ServerCall[Request, Response] => SyncIO[Cancel],
    options: ServerOptions
  ): ServerCallHandler[Request, Response] =
    new ServerCallHandler[Request, Response] {
      private val opt = options.callOptionsFn(ServerCallOptions.default)

      def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] = {
        for {
          responder <- Fs2ServerCall.setup(opt, call)
          _ <- responder.request(1) //prefetch size
          ch <- OneShotChannel.empty[Request]
          cancel <- start(ch.stream.through(requestOnPull(call)), headers)(responder)
        } yield mkListener(ch, cancel)
      }.unsafeRunSync()
    }

  def unary[F[_] : Async, Request, Response](
    impl: (Stream[F, Request], Metadata) => F[Response],
    options: ServerOptions,
    dispatcher: Dispatcher[F]
  ): ServerCallHandler[Request, Response] =
    mkHandler[F, Request, Response]((req, h) => _.unary(impl(req, h), dispatcher), options)

  def stream[F[_] : Async, Request, Response](
    impl: (Stream[F, Request], Metadata) => Stream[F, Response],
    options: ServerOptions,
    dispatcher: Dispatcher[F]
  ): ServerCallHandler[Request, Response] =
    mkHandler[F, Request, Response]((req, h) => _.stream(impl(req, h), dispatcher), options)


}