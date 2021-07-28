/*
 * Copyright (c) 2021 the Vector Clocks contributors.
 * See the project homepage at: https://splitbrain.io/vector-clocks/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package splitbrain.clocks

import java.util.concurrent.TimeUnit

import cats.Order

import scala.concurrent.duration.TimeUnit
import Clock._

/**
  * Implementation of a Hybrid Logical Clock (HLC) described by Kulkarni, Demirbas et al in
  * "Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases".
  *
  * Hybrid logical clocks are based on standard wall clock time (e.g. that provided by
  * `System.currentTimeMillis`), but use an additional counter in order to ensure monotonicity.
  *
  * They improve upon other logical clocks (such as Lamport clocks) because they offer minimal
  * drift from their underlying wall clock over time. I.e. a single timestamp value may be used
  * both to report on real time events and infer causality.
  *
  * The timestamps stored by HybridLogicalClock objects are of millisecond precision, though the
  * counter may be used to derive ordering between several objects with the same timestamp.
  *
  * TODO: Implement a merge which also compares to local physical timestamp
  *   Is that equiv to merge and tick? that will increment the counter but I'm not certain that matters.
  *   Eventually the physical timestamp will overtake and the counter will reset to 0
  */
final class HybridLogicalClock[C: WallClock] private (private val physicalClock: C, val timestamp: Long, val counter: Int)
  extends Equals {

  def tick: HybridLogicalClock[C] = {
    val tickedClock = physicalClock.tick
    val physicalTimeNow = tickedClock.latestWallTime(TimeUnit.MILLISECONDS)

    val previousTimestamp = timestamp
    val nextTimestamp = Math.max(previousTimestamp, physicalTimeNow)

    val nextCounter = if (nextTimestamp == previousTimestamp) counter + 1 else 0
    new HybridLogicalClock[C](tickedClock, nextTimestamp, nextCounter)
  }

  def merge(remote: HybridLogicalClock[C]): HybridLogicalClock[C] = {
    val previousTimestampLocal = timestamp
    val previousTimestampRemote = remote.timestamp
    val nextTimestamp = Math.max(previousTimestampLocal, previousTimestampRemote)

    val nextCounter = {
      if (previousTimestampLocal == previousTimestampRemote)
        Math.max(counter, remote.counter) + 1
      else if (nextTimestamp == previousTimestampLocal)
        counter + 1
      else
        remote.counter + 1
    }

    new HybridLogicalClock[C](physicalClock, nextTimestamp, nextCounter)
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[HybridLogicalClock[_]]

  override def equals(other: Any): Boolean =
    (this eq other.asInstanceOf[AnyRef]) || (other match {
      case that: HybridLogicalClock[C] if that.canEqual(this) => timestamp == that.timestamp && counter == that.counter
      case _ => false
    })

  override def hashCode(): Int = {
    val state = Seq(timestamp, counter.toLong)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object HybridLogicalClock extends LowPriorityInstances {

  import Clock._

  def apply[C: Clock](physicalClock: C): HybridLogicalClock[C] =
    new HybridLogicalClock[C](physicalClock, Clock[C].latestTime(physicalClock), 0)

  def unapply[C](clock: HybridLogicalClock[C]): Option[(Long, Int)] = Some((clock.timestamp, clock.counter))

  implicit val clockInstanceForHybridSystem: Clock[HybridLogicalClock[SystemClock]] =
    new Clock[HybridLogicalClock[SystemClock]] {
      override val order: Order[HybridLogicalClock[SystemClock]] = orderInstanceForHybridLogical[SystemClock]
      override val startOfTime: HybridLogicalClock[SystemClock] =
        HybridLogicalClock[SystemClock](Clock[SystemClock].startOfTime)

      override def latestTime(c: HybridLogicalClock[SystemClock], units: TimeUnit): Long =
        units.convert(c.timestamp, TimeUnit.MILLISECONDS)

      override def tick(c: HybridLogicalClock[SystemClock]): HybridLogicalClock[SystemClock] = c.tick

      override def merge(
        l: HybridLogicalClock[SystemClock],
        r: HybridLogicalClock[SystemClock]): HybridLogicalClock[SystemClock] = l merge r
    }
}

trait LowPriorityInstances {
  implicit def clockInstanceForHybrid[C: Clock]: Clock[HybridLogicalClock[C]] =
    new Clock[HybridLogicalClock[C]] {
      override val order: Order[HybridLogicalClock[C]] = orderInstanceForHybridLogical[C]
      override val startOfTime: HybridLogicalClock[C] = HybridLogicalClock[C](Clock[C].startOfTime)

      override def latestTime(c: HybridLogicalClock[C], units: TimeUnit): Long =
        units.convert(c.timestamp, TimeUnit.MILLISECONDS)

      override def tick(c: HybridLogicalClock[C]): HybridLogicalClock[C] = c.tick

      override def merge(l: HybridLogicalClock[C], r: HybridLogicalClock[C]): HybridLogicalClock[C] = l merge r
    }

  protected def orderInstanceForHybridLogical[C]: Order[HybridLogicalClock[C]] =
    Order.from((l, r) => {
      val diff = l.timestamp - r.timestamp
      if (diff == 0)
        l.counter - r.counter
      else
        diff.toInt
    })
}
