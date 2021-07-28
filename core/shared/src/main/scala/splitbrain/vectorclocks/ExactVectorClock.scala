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

package splitbrain.vectorclocks

import java.util.concurrent.TimeUnit

import cats.Eq
import cats.Monoid
import cats.Show
import cats.implicits._
import ExactVectorClock.Timestamp
import cats.PartialOrder
import splitbrain.clocks.Clock
import splitbrain.vectorclocks.ExactVectorClock.Equal
import splitbrain.vectorclocks.ExactVectorClock.HappensAfter
import splitbrain.vectorclocks.ExactVectorClock.HappensBefore
import splitbrain.vectorclocks.ExactVectorClock.HappensConcurrent

import scala.collection.immutable.HashMap

/**
  * TODO:
  *   - Add better Scaladoc
  *   - Add binary encoding (with scodec?). Make sure to include magic bytes and a version number so that in future the format can change
  *
  * @param clock
  * @param timestamps
  * @tparam N
  */
final class ExactVectorClock[N, C: Clock] private (clock: C, val timestamps: HashMap[N, Timestamp]) extends Equals {
  import ExactVectorClock._

  def isEmpty: Boolean = timestamps.isEmpty

  def nonEmpty: Boolean = timestamps.nonEmpty

  def put(node: N): ExactVectorClock[N, C] = {
    val tickedClock = Clock[C].tick(clock)
    val timeNow = Clock[C].latestTime(tickedClock)
    ExactVectorClock(clock, timestamps + (node -> timeNow))
  }

  def remove(node: N): ExactVectorClock[N, C] = ExactVectorClock(clock, timestamps - node)

  def compareTo(that: ExactVectorClock[N, C]): Relationship = {
    if (timestamps == that.timestamps)
      Equal
    else if (this isLessThan that)
      HappensBefore
    else if (that isLessThan this)
      HappensAfter
    else
      HappensConcurrent
  }

  private def isLessThan(that: ExactVectorClock[N, C]): Boolean = {
    val clockZero = Clock[C].startOfTime
    val timeZero = Clock[C].latestTime(clockZero)
    val allLeftLTE = timestamps.forall { case (id, ts) => ts <= that.timestamps.getOrElse(id, timeZero) }
    val oneRightGT = that.timestamps.exists { case (id, ts) => ts > timestamps.getOrElse(id, timeZero) }
    allLeftLTE && oneRightGT
  }

  def merge(that: ExactVectorClock[N, C]): ExactVectorClock[N, C] = {
    if (this == that) {
      this
    } else {
      val mergedTimestamps = timestamps.merged(that.timestamps) { case ((lk, lv), (_, rv)) => lk -> (lv max rv) }
      ExactVectorClock(clock, mergedTimestamps)
    }
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ExactVectorClock[_, _]]

  override def equals(other: Any): Boolean =
    (this eq other.asInstanceOf[AnyRef]) || (other match {
      case that: ExactVectorClock[N, C] if that.canEqual(this) => (that compareTo this) == Equal
      case _ => false
    })

  override def hashCode: Int = timestamps.hashCode
}

object ExactVectorClock extends VectorClockInstances {

  def apply[N, C: Clock](pClock: C, timestamps: HashMap[N, Timestamp]): ExactVectorClock[N, C] =
    new ExactVectorClock(pClock, timestamps)

  /**
    * Logical timestamp, represented as a simple long
    */
  type Timestamp = Long

  /**
    * Relationships between Vector clocks.
    * Not the same as a simple ordering, because there are 4 states rather than 3:
    *
    * HAPPENS-BEFORE,
    * HAPPENS-AFTER,
    * EQUAL,
    * HAPPENS-CONCURRENT
    */
  sealed trait Relationship
  case object HappensBefore extends Relationship
  case object HappensAfter extends Relationship
  case object Equal extends Relationship
  case object HappensConcurrent extends Relationship
}

trait VectorClockInstances {

  implicit def vClockEq[N, C]: Eq[ExactVectorClock[N, C]] = Eq.fromUniversalEquals

  implicit def vClockMonoid[N, C: Clock]: Monoid[ExactVectorClock[N, C]] =
    new Monoid[ExactVectorClock[N, C]] {
      override def empty: ExactVectorClock[N, C] =
        ExactVectorClock[N, C](Clock[C].startOfTime, HashMap.empty[N, Timestamp])

      override def combine(x: ExactVectorClock[N, C], y: ExactVectorClock[N, C]): ExactVectorClock[N, C] = x merge y
    }

  implicit def vClockShow[N: Show, C]: Show[ExactVectorClock[N, C]] =
    Show.show { vClock =>
      vClock.timestamps.iterator
        .map({ case (k, v) => s"${k.show} -> $v" })
        .mkString("VectorClock(", ", ", ")")
    }

  implicit def vClockPartialOrder[N, C]: PartialOrder[ExactVectorClock[N, C]] =
    (x: ExactVectorClock[N, C], y: ExactVectorClock[N, C]) =>
      x.compareTo(y) match {
        case HappensBefore => -1.0d
        case HappensAfter => 1.0d
        case Equal => 0.0d
        case HappensConcurrent => Double.NaN
      }
}
