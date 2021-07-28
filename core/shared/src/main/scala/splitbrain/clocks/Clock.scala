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

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.specialized
import scala.concurrent.duration.TimeUnit
import cats.Order
import splitbrain.clocks.Clock.FakeClock
import splitbrain.clocks.Clock.SystemClock

/**
  * Simple type class representing clock like behaviour.
  *
  * Implementations should be immutable, producing a new instance on each `tick()`.
  * They do not have to have a physical representation of time (i.e. may be purely logical).
  *
  * Note: We don't worry about timezones etc... here.  
  * Where clocks are physical, their timestamps should be relative to the unix epoch in UTC (+0000).
  * Converting to other timezones may then be done by the user.
  *
  * Note the second: Despite the presence of `empty` (`startOfTime`) and `combine` (`merge`) there
  * is *not* a well defined [[cats.Monoid]] instance for all possible `Clock`s.
  *
  * This is because some clock implementations may increment their underlying timestamps even when
  * merging an instance with `startOfTime`, so `startOfTime` is not a proper identity.
  */
trait Clock[@specialized(Long) C] {

  /**
    * All instances of the clock typeclass must provide an instance fo the [[cats.kernel.Order]]
    * typeclass as well. 
    */
  def order: Order[C]

  /**
    * A [[Clock]] instance whose `timeNow(units)` method will return the smallest possible value for 
    * any clock of the same type `C`. 
    * 
    * Must be less than all other possible values of `C` according to the provided `Order[C]` instance.
    *
    * @return a clock instance set to the earliest time that clock type can have
    */
  def startOfTime: C

  /**
    * Returns the current "time" value of the clock `c`. This time may be phsyical or logical.
    * If physical, the returned value should correspond to the number of millis since the unix-epoch.
    *
    * @param c the clock instance
    * @return a long `n` representing the "time"
    */    
  def latestTime(c: C): Long

  /**
    * Produce a new clock instance with updated time. 
    */
  def tick(c: C): C

  /**
    * Return the more recent of the the two clock instances `l` and `r`
    */
  def merge(l: C, r: C): C = order.max(l, r)
}

/**
  * Simple typeclass implemented by the subset of [[Clock]] instances which 
  * are able to produce physical/wall-clock values for time.
  * 
  * Units may be specified by a caller, but all times returned are assumed to
  * be relative to the unix epoch, in UTC (+0000) timezone. 
  */
trait WallClock[C] extends Clock[C] { self =>
  
  /**
    * Returns the current physical/wall-clock time value of the clock `c`
    * 
    * Note that this method is side-effect free. This means that the value returned corresponds to the
    * "time" of the clock `c` the last time the `tick()` method was called. If you want the time *now*,
    * rather than the *latest* time, you must call `tick()` again first.
    *
    * @param c the clock instance 
    * @param units the units for which to count time, 
    * @return a long `n` representing the time (in specified units) since the unix-epoch
    */
    def latestWallTime(c: C, units: TimeUnit): Long = units.convert(self.latestTime(c), units)
}

object Clock extends ClockInstances with ClockSyntax {
  /**
    * Implicitly resolve `Clock` instance for type `C`
    */
  @inline def apply[C: Clock]: Clock[C] = implicitly[Clock[C]]

  /**
    * Common clock types
    */
  final case class SystemClock(now: Instant)
  final case class FakeClock(now: Long, timestamps: LazyList[Long], unit: TimeUnit)

  object SystemClock {
    /**
      * Factory method, for automatically providing the most common argument to [[SystemClock]]'s constructor
      */
    def now: SystemClock = SystemClock(Instant.now)
  }
}

trait ClockInstances {

  implicit def orderInstanceForClock[C: Clock]: Order[C] = Clock[C].order

  implicit val clockInstanceForSystem: Clock[SystemClock] = new Clock[SystemClock] {
    override val order: Order[SystemClock] = Order.fromLessThan((l, r) => l.now isBefore r.now)
    override val startOfTime: SystemClock = SystemClock(Instant.EPOCH)

    override def latestTime(c: SystemClock): Long = c.now.toEpochMilli

    override def tick(clock: SystemClock): SystemClock = clock.copy(Instant.now())
  }

  implicit val clockInstanceForFake: Clock[FakeClock] = new Clock[FakeClock] {
    override val order: Order[FakeClock] = Order.by(_.now)
    override val startOfTime: FakeClock = FakeClock(0L, LazyList.empty[Long], TimeUnit.MILLISECONDS)

    override def latestTime(c: FakeClock, units: TimeUnit): Long = units.convert(c.now, c.unit)

    override def tick(c: FakeClock): FakeClock = c.copy(c.timestamps.head, c.timestamps.tail)
  }

  implicit val clockInstanceForLong: Clock[Long] = new Clock[Long] {
    override val order: Order[Long] = Order[Long]
    override val startOfTime: Long = 0L

    override def latestTime(c: Long): Long = c

    override def tick(c: Long): Long = c+1
  }
}

trait ClockSyntax {
  implicit final def clockSyntax[C: Clock](theClock: C): ClockOps[C] = new ClockOps[C](theClock)
}

final class ClockOps[C: Clock](theClock: C) {
  def tick: C = Clock[C].tick(theClock)
  def latestTime: Long = Clock[C].latestTime(theClock)
  def latestWallTime(units: TimeUnit)(implicit ev: WallClock[C]): Long = 
    ev.latestWallTime(theClock, units)
}
