package fs2.grpc.internal.client

import cats.effect.Sync
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import fs2._
import fs2.grpc.internal.server.Fs2StatefulServerCall.Cancel
import io.grpc._

class Fs2StatefulClientCall[F[_], I, O](
    call: ClientCall[I, O],
    dispatcher: Dispatcher[F]
)(implicit F: Async[F]) {

  def stream(response: Stream[F, O])(implicit F: Sync[F]): Cancel = ???

  def unary(response: F[O])(implicit F: Sync[F]): Cancel = ???

}

object Fs2StatefulClientCall {
  type Cancel = () => Any

}