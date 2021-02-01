/*
 * Copyright (c) 2020 the Vector Clocks contributors.
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

import java.util.concurrent.atomic.AtomicLong

import cats.Eq
import cats.Monoid
import cats.Show
import cats.implicits._
import ExactVectorClock.Timestamp
import cats.PartialOrder
import splitbrain.vectorclocks.ExactVectorClock.Equal
import splitbrain.vectorclocks.ExactVectorClock.HappensAfter
import splitbrain.vectorclocks.ExactVectorClock.HappensBefore
import splitbrain.vectorclocks.ExactVectorClock.HappensConcurrent

import scala.collection.immutable.HashMap

/**
  * TODO:
  *   - Add better Scaladoc
  *   - Add syntax trait
  *   - Add binary encoding (with scodec?). Make sure to include magic bytes and a version number so that in future the format can change
  *   - Extract timestamp behaviour to Clock type or typeclass
  *   - Remove global counter and implement separate field for logical clock so we don't drift from physical time
  *
  * @param pClock
  * @param timestamps
  * @param counter
  * @tparam Node
  */
final class ExactVectorClock[Node] private(pClock: () => Timestamp,
                                           val timestamps: HashMap[Node,Timestamp],
                                           counter: AtomicLong) extends Equals {
  import ExactVectorClock._

  /**
    * We take a hybrid logical clock approach to producing timestamps from this clock.
    * I.e. where possible we use the provided physical clock, but we always preserve the logical clock
    * guarantee of growing monotonically.
    */
  private def timestamp: Timestamp = {
    var lastTimestamp = timeZero
    var nextTimestamp = timeZero

    do {
      val currentTimestamp = pClock()
      lastTimestamp = counter.get
      nextTimestamp = if (currentTimestamp > lastTimestamp) currentTimestamp else lastTimestamp + 1;
    } while(!counter.compareAndSet(lastTimestamp, nextTimestamp))

    nextTimestamp
  }

  def isEmpty: Boolean = timestamps.isEmpty

  def nonEmpty: Boolean = timestamps.nonEmpty

  def put(node: Node): ExactVectorClock[Node] = ExactVectorClock(pClock, timestamps + (node -> timestamp), counter)

  def remove(node: Node): ExactVectorClock[Node] = ExactVectorClock(pClock, timestamps - node, counter)

  def compareTo(that: ExactVectorClock[Node]): Relationship = {
    if (timestamps == that.timestamps)
      Equal
    else if (this isLessThan that)
      HappensBefore
    else if (that isLessThan this)
      HappensAfter
    else
      HappensConcurrent
  }

  private def isLessThan(that: ExactVectorClock[Node]): Boolean = {
    val allLeftLTE = timestamps.forall { case (id, ts) => ts <= that.timestamps.getOrElse(id, timeZero) }
    val oneRightGT = that.timestamps.exists { case (id, ts) => ts > timestamps.getOrElse(id, timeZero) }
    allLeftLTE && oneRightGT
  }

  def merge(that: ExactVectorClock[Node]): ExactVectorClock[Node] = {
    if ( this == that ) {
      this
    } else {
      val mergedTimestamps = timestamps.merged(that.timestamps) { case ((lk,lv), (_,rv)) => lk -> (lv max rv) }
      ExactVectorClock(pClock, mergedTimestamps, counter)
    }
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ExactVectorClock[_]]

  override def equals(other: Any): Boolean =
    (this eq other.asInstanceOf[AnyRef]) || (other match {
      case that: ExactVectorClock[Node] if that.canEqual(this) => (that compareTo this) == Equal
      case _ => false
    })

  override def hashCode: Int = timestamps.hashCode
}

object ExactVectorClock extends VectorClockInstances {

  private val globalCounter = new AtomicLong(0)

  def apply[Node](pClock: () => Timestamp = System.currentTimeMillis,
                  timestamps: HashMap[Node,Timestamp] = HashMap.empty[Node, Timestamp],
                  counter: AtomicLong = globalCounter): ExactVectorClock[Node] =
    new ExactVectorClock(pClock, timestamps, counter)

  /**
    * Logical timestamp, represented as a simple long
    */
  type Timestamp = Long
  final val timeZero: Timestamp = 0L

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

  implicit def vClockEq[Node]: Eq[ExactVectorClock[Node]] = Eq.fromUniversalEquals

  implicit def vClockMonoid[Node]: Monoid[ExactVectorClock[Node]] = new Monoid[ExactVectorClock[Node]] {
    override def empty: ExactVectorClock[Node] = ExactVectorClock()

    override def combine(x: ExactVectorClock[Node], y: ExactVectorClock[Node]): ExactVectorClock[Node] = x merge y
  }

  implicit def vClockShow[Node : Show]: Show[ExactVectorClock[Node]] = Show.show { vClock =>
    vClock.timestamps.iterator
      .map({ case (k,v) => s"${k.show} -> $v" })
      .mkString("VectorClock(", ", ", ")")
  }

  implicit def vClockPartialOrder[Node]: PartialOrder[ExactVectorClock[Node]] =
    (x: ExactVectorClock[Node], y: ExactVectorClock[Node]) => x.compareTo(y) match {
      case HappensBefore => -1.0D
      case HappensAfter => 1.0D
      case Equal => 0.0D
      case HappensConcurrent => Double.NaN
    }
}
