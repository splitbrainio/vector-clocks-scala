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

import java.util.UUID

import cats.implicits._
import cats.kernel.laws.discipline.EqTests
import arbitraries._
import cats.kernel.laws.discipline.MonoidTests
import cats.kernel.laws.discipline.PartialOrderTests
import org.scalacheck.Arbitrary.arbFunction1
import splitbrain.vectorclocks.SimpleVectorClock.Equal
import splitbrain.vectorclocks.SimpleVectorClock.HappensConcurrent

import scala.collection.immutable.HashMap

class SimpleVectorClockSpec extends VectorClocksSuite {

  // Check typeclass laws
  checkAll("VectorClock.MonoidLaws", MonoidTests[SimpleVectorClock[UUID]].monoid)
  checkAll("VectorClock.EqLaws", EqTests[SimpleVectorClock[UUID]].eqv)
  checkAll("VectorClock.PartialOrderLaws", PartialOrderTests[SimpleVectorClock[UUID]].partialOrder)

  // Test behaviours
  test("put should add a node to a vector clock's entries") {
    val timestamp = 1607820163000L
    val fakeClock = () => timestamp
    val v = SimpleVectorClock[Int](pClock = fakeClock)
    assert(v.timestamps.isEmpty)
    val v2 = v.put(1)
    assert(v2.timestamps.nonEmpty)
    assert(v2.timestamps == Map(1->timestamp))
  }

//  test("put should respect monotonicity") {
//
//  }

  test("remove should remove a node from a vector clock's entries") {
    val time = 1607820163000L
    val timestamps = HashMap(1 -> time, 7 -> (time + 3200L))
    val v = SimpleVectorClock[Int](timestamps = timestamps)
    assert(v.timestamps.keySet == Set(1,7))

    val v1 = v.remove(1)
    assert(!v1.timestamps.contains(1))
  }

  test( "show should print a human readable output") {
    val timestamp = 1610352590000L
    val fakeClock = () => timestamp
    val v = SimpleVectorClock[Int](pClock = fakeClock)
      .put(1)
      .put(2)
    assert(v.show == s"VectorClock(1 -> ${timestamp}, 2 -> ${timestamp+1})")
  }

  test("empty vector clocks should be equal") {
    val v1 = SimpleVectorClock()
    val v2 = SimpleVectorClock()
    assert(v1.compareTo(v2) == Equal)
  }

  test("unrelated vector clocks should be concurrent") {
    val v1 = SimpleVectorClock[Int]()
      .put(1)
    val v2 = SimpleVectorClock[Int]()
      .put(2)
    assert(v1.compareTo(v2) == HappensConcurrent)
  }

//  test("related vector clocks should be correctly ordered") {
//
//  }
//
//  test("vector clocks should be immutable") {
//
//  }
}
