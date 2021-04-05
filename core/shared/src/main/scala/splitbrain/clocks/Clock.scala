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

package splitbrain.clocks

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.TimeUnit
import cats.Order
import splitbrain.clocks.Clock.FakeClock
import splitbrain.clocks.Clock.SystemClock

/**
  * Simple type representing clock like behaviour.
  *
  * Implementations should be immutable, producing a new instance on each `tick()`.
  * They should also have a physical representation of time (i.e. shouldn't be purely logical)
  *
  * Note: We don't worry about timezones etc... here.
  * All timestamps boil down to millis since the unix epoch. Timezone handling can be done by the user.
  *
  * Note the second: Despite the presence of `empty` (`startOfTime`) and `combine` (`merge`) there
  * is *not* a well defined [[cats.Monoid]] instance for all possible `Clock`s.
  *
  * This is because some clock implementations may increment their underlying timestamps even when
  * merging an instance with `startOfTime`, so `startOfTime` is not a proper identity.
  *
  * TODO: Scaladoc for methods
  */
trait Clock[C] {

  def order: Order[C]

  def startOfTime: C

  def timeNow(c: C, units: TimeUnit): Long

  def tick(c: C): C

  def merge(l: C, r: C): C = order.max(l, r)
}

object Clock extends ClockInstances {

  /**
    * Implicitly resolve `Clock` instance for type `C`
    */
  @inline def apply[C: Clock]: Clock[C] = implicitly[Clock[C]]

  final case class SystemClock(now: Instant)

  object SystemClock {
    def now: SystemClock = SystemClock(Instant.now)
  }

  final case class FakeClock(now: Long, timestamps: LazyList[Long], unit: TimeUnit)
}

trait ClockInstances {

  implicit def orderInstanceForClock[C: Clock]: Order[C] = Clock[C].order

  implicit val clockInstanceForSystem: Clock[SystemClock] = new Clock[SystemClock] {
    override val order: Order[SystemClock] = Order.fromLessThan((l, r) => l.now isBefore r.now)
    override val startOfTime: SystemClock = SystemClock(Instant.EPOCH)

    override def timeNow(clock: SystemClock, units: TimeUnit): Long = units.convert(clock.now.toEpochMilli, TimeUnit.MILLISECONDS)

    override def tick(clock: SystemClock): SystemClock = clock.copy(Instant.now())
  }

  implicit val clockInstanceForFake: Clock[FakeClock] = new Clock[FakeClock] {
    override val order: Order[FakeClock] = Order.by(_.now)
    override val startOfTime: FakeClock = FakeClock(0L, LazyList.empty[Long], TimeUnit.MILLISECONDS)

    override def timeNow(c: FakeClock, units: TimeUnit): Long = units.convert(c.now, c.unit)

    override def tick(c: FakeClock): FakeClock = c.copy(c.timestamps.head, c.timestamps.tail)
  }
}
