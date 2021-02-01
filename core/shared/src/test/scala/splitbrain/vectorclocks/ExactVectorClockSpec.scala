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
import splitbrain.vectorclocks.ExactVectorClock.Equal
import splitbrain.vectorclocks.ExactVectorClock.HappensConcurrent

import scala.collection.immutable.HashMap
import scala.collection.mutable

class ExactVectorClockSpec extends VectorClocksSuite {

  // Check typeclass laws
  checkAll("VectorClock.MonoidLaws", MonoidTests[ExactVectorClock[UUID]].monoid)
  checkAll("VectorClock.EqLaws", EqTests[ExactVectorClock[UUID]].eqv)
  checkAll("VectorClock.PartialOrderLaws", PartialOrderTests[ExactVectorClock[UUID]].partialOrder)

  // Test behaviours
  test("put should add a node to a vector clock's entries") {
    val timestamp = 1607820163000L
    val fakeClock = () => timestamp
    val v = ExactVectorClock[Int](pClock = fakeClock)
    assert(v.timestamps.isEmpty)
    val v2 = v.put(1)
    assert(v2.timestamps.nonEmpty)
    assert(v2.timestamps == Map(1->timestamp))
  }

  test("put should respect monotonicity") {
    val maxTimestamp = 1612166159000L
    val minTimestamp = 1612166158000L
    val reverseTimestamps = (minTimestamp to maxTimestamp).reverse.toList
    val queue = mutable.Queue.from(reverseTimestamps)
    val reverseClock = () => queue.dequeue()
    val v = ExactVectorClock[Int](reverseClock)
    val vNew = (0 until 10).foldLeft(v) { (vDelta,i) => vDelta.put(i) }

    assert(vNew.timestamps.values.min >= maxTimestamp)
    assert(vNew.timestamps.values.toSet.size == 10)

    val sortedByNode = vNew.timestamps.toSeq.sortBy(_._1)
    val sortedByTimestamp = vNew.timestamps.toSeq.sortBy(_._2)

    assert(sortedByNode == sortedByTimestamp)
  }

  test("remove should remove a node from a vector clock's entries") {
    val time = 1607820163000L
    val timestamps = HashMap(1 -> time, 7 -> (time + 3200L))
    val v = ExactVectorClock[Int](timestamps = timestamps)
    assert(v.timestamps.keySet == Set(1,7))

    val v1 = v.remove(1)
    assert(!v1.timestamps.contains(1))
  }

  //TODO: Fix periodic failures by removing
  test( "show should print a human readable output") {
    val timestamp = 1610352590000L
    val fakeClock = () => timestamp
    val v = ExactVectorClock[Int](pClock = fakeClock)
      .put(1)
      .put(2)
    assert(v.show == s"VectorClock(1 -> ${timestamp}, 2 -> ${timestamp+1})")
  }

  test("empty vector clocks should be equal") {
    val v1 = ExactVectorClock()
    val v2 = ExactVectorClock()
    assert(v1.isEmpty && v2.isEmpty)
    assert(v1.compareTo(v2) == Equal)
  }

  test("unrelated vector clocks should be concurrent") {
    val v1 = ExactVectorClock[Int]()
      .put(1)
    val v2 = ExactVectorClock[Int]()
      .put(2)
    assert(v1.compareTo(v2) == HappensConcurrent)
  }

//  test("related vector clocks should be correctly ordered") {
//
//  }

  test("vector clocks should be immutable") {
    val v1 = ExactVectorClock[Int]()
    val v2 = v1.put(1)
    assert(v1 != v2)
    assert(v1.isEmpty)
    assert(v2.nonEmpty)
  }
}
