package fs2.grpc.internal.client

import cats.effect.kernel.Async
import cats.effect.kernel.Resource.ExitCase
import fs2._
import fs2.grpc.client.ClientOptions
import fs2.grpc.internal.UnsafeChannel
import io.grpc.ClientCall
import io.grpc.Metadata
import io.grpc.Status
import scala.util.control.NonFatal


object StreamClientCallHandler {
  private def mkListener[Response](
    channel: UnsafeChannel[Either[Throwable, Response]],
    options: ClientOptions
  ): ClientCall.Listener[Response] =
    new ClientCall.Listener[Response] {
      override def onMessage(message: Response): Unit =
        channel.send(Right(message))

      override def onClose(status: Status, trailers: Metadata): Unit = {
        if (!status.isOk) {
          try {
            channel.send(Left(options.errorAdapter.applyOrElse(status.asRuntimeException(trailers), a => a)))
          } catch {
            case NonFatal(e) => channel.send(Left(e))
          }
        }
        channel.close()
      }
    }


  def unary[F[_], Request, Response](
    call: ClientCall[Request, Response],
    options: ClientOptions,
    message: Request,
    headers: Metadata
  )(implicit F: Async[F]): Stream[F, Response] =
    Stream.force {
      F.delay {
        val channel = UnsafeChannel.empty[Either[Throwable, Response]]
        call.start(mkListener(channel, options), headers)
        call.sendMessage(message)
        call.halfClose()
        call.request(options.prefetchN.max(1))
        channel.stream.mapChunks { chunks =>
          call.request(chunks.size)
          chunks
        }.onFinalizeCase {
          case ExitCase.Canceled => F.delay(call.cancel(null, null))
          case _ => F.unit
        }
      }
    }.through(unAttempt)


  def stream[F[_], Request, Response](
    call: ClientCall[Request, Response],
    options: ClientOptions,
    message: Stream[F, Request],
    headers: Metadata
  )(implicit F: Async[F]): Stream[F, Response] = ???



  private def unAttempt[F[_] : RaiseThrowable, A]: Pipe[F, Either[Throwable, A], A] =
    _.repeatPull(_.uncons.flatMap {
      case Some((hd, tail)) =>
        hd.indexWhere(_.isLeft) match {
          case None =>
            Pull.output(hd.map { case Right(value) => value }).as(Some(tail))
          case Some(idx) =>
            Pull.output(hd.take(idx).map { case Right(value) => value }) >>
              Pull.raiseError[F](hd(idx).asInstanceOf[Left[Throwable, A]].value).as(None)
        }
      case None => Pull.pure(None)
    })
}