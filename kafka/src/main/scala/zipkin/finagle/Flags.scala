package zipkin.finagle

import java.net.InetSocketAddress

import com.twitter.app.GlobalFlag
import com.twitter.finagle.zipkin.core.Sampler

object hosts extends GlobalFlag[Seq[InetSocketAddress]](
  List(new InetSocketAddress("localhost", 9092)),
  "Initial set of kafka servers to connect to, rest of cluster will be discovered (comma separated)")

object initialSampleRate extends GlobalFlag[Float](
  Sampler.DefaultSampleRate,
  "Initial sample rate in the range [0.0 - 1.0]")