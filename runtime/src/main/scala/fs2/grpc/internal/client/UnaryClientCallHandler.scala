package fs2.grpc.internal.client

import cats.effect.kernel.Async
import fs2.grpc.client.ClientOptions
import fs2._
import io.grpc.ClientCall
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusRuntimeException
import scala.util.control.NonFatal


object UnaryClientCallHandler {
  private def mkListener[Response](
    callback: Either[Throwable, Response] => Unit,
    options: ClientOptions
  ): ClientCall.Listener[Response] =
    new ClientCall.Listener[Response] {
      private var received: Response = _
      private var done = false

      private def resume(response: Response): Unit = {
        if (!done) {
          done = true
          callback(Right(response))
        }
      }

      private def resumeError(status: StatusRuntimeException): Unit = {
        if (!done) {
          done = true
          try {
            callback(Left(options.errorAdapter.applyOrElse(status, e => e)))
          } catch {
            case NonFatal(e) => callback(Left(e))
          }
        }
      }

      override def onMessage(message: Response): Unit = {
        if (received == null) {
          received = message
        } else {
          resumeError(Status.INTERNAL
            .withDescription("More than one value received for unary call")
            .asRuntimeException())
        }
      }

      override def onClose(status: Status, trailers: Metadata): Unit = {
        if (status.isOk) {
          if (received == null) {
            resumeError(Status.INTERNAL
              .withDescription("No value received for unary call")
              .asRuntimeException(trailers))
          } else {
            resume(received)
          }
        } else {
          resumeError(status.asRuntimeException(trailers))
        }
      }
    }


  def unary[F[_], Request, Response](
    call: ClientCall[Request, Response],
    options: ClientOptions,
    message: Request,
    headers: Metadata
  )(implicit F: Async[F]): F[Response] = F.async[Response] {
    cb =>
      F.delay {
        call.sendMessage(message)
        call.halfClose()
        call.start(mkListener[Response](cb, options), headers)
        call.request(2)
        Some(F.delay(call.cancel(null, null)))
      }
  }

  def stream[F[_], Request, Response](
    call: ClientCall[Request, Response],
    options: ClientOptions,
    message: Stream[F, Request],
    headers: Metadata
  )(implicit F: Async[F]): F[Response] = F.async[Response] {
    cb =>
      F.defer {
        call.start(mkListener[Response](cb, options), headers)
        call.request(2)
        F.as(
          message.map(call.sendMessage).onFinalize(F.delay(call.halfClose())).compile.drain,
          Some(F.delay(call.cancel(null, null))))
      }
  }
}