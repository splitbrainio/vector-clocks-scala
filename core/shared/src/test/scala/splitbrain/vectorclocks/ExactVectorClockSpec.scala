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

import java.util.UUID

import cats.implicits._
import cats.kernel.laws.discipline.EqTests
import arbitraries._
import cats.kernel.laws.discipline.MonoidTests
import cats.kernel.laws.discipline.PartialOrderTests
import org.scalacheck.Arbitrary.arbFunction1
import splitbrain.SplitBrainSuite
import splitbrain.vectorclocks.ExactVectorClock.Equal
import splitbrain.vectorclocks.ExactVectorClock.HappensConcurrent
import splitbrain.clocks.Clock._

import scala.collection.immutable.HashMap

class ExactVectorClockSpec extends SplitBrainSuite {

  // Check typeclass laws
  checkAll("VectorClock.MonoidLaws", MonoidTests[ExactVectorClock[UUID,Long]].monoid)
  checkAll("VectorClock.EqLaws", EqTests[ExactVectorClock[UUID,Long]].eqv)
  checkAll("VectorClock.PartialOrderLaws", PartialOrderTests[ExactVectorClock[UUID,Long]].partialOrder)

  // Test behaviours
  test("put should add a node to a vector clock's entries") {
    val timestamp = 12793472973L
    val v = ExactVectorClock[Int,Long](timestamp)
    assert(v.timestamps.isEmpty)
    val v2 = v.put(1)
    assert(v2.timestamps.nonEmpty)
    assert(v2.timestamps == Map(1 -> timestamp))
  }


  test("remove should remove a node from a vector clock's entries") {
    val v = ExactVectorClock.empty[Int,Long]
        .put(1)
        .put(7)
    assert(v.timestamps.keySet == Set(1, 7))

    val v1 = v.remove(1)
    assert(!v1.timestamps.contains(1))
  }

  test("show should print a human readable output") {
    val timestamp = 3200L 
    val v = ExactVectorClock[Int,Long](timestamp)
      .put(1)
      .put(2)
    assert(v.show == s"VectorClock(1 -> ${timestamp}, 2 -> ${timestamp + 1})")
  }

  test("empty vector clocks should be equal") {
    val v1 = ExactVectorClock.empty[Int,Long]
    val v2 = ExactVectorClock.empty[Int,Long]
    assert(v1.isEmpty && v2.isEmpty)
    assert(v1.compareTo(v2) == Equal)
  }

  test("unrelated vector clocks should be concurrent") {
    val v1 = ExactVectorClock.empty[Int,Long]
      .put(1)
    val v2 = ExactVectorClock.empty[Int,Long]
      .put(2)
    assert(v1.compareTo(v2) == HappensConcurrent)
  }

//  test("related vector clocks should be correctly ordered") {
//
//  }

  test("vector clocks should be immutable") {
    val v1 = ExactVectorClock.empty[Int,Long]
    val v2 = v1.put(1)
    assert(v1 != v2)
    assert(v1.isEmpty)
    assert(v2.nonEmpty)
  }
}
