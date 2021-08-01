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

import splitbrain.SplitBrainSuite

class HybridLogicalClockSpec extends SplitBrainSuite {


//   test("put should respect monotonicity") {
//     //TODO: move this test to the HybridLogicalClocksSpec.
//     //  Replace with a test adding multiple timestamps for the same node and see that the value goes up
//     val maxTimestamp = 1612166159000L
//     val minTimestamp = 1612166158000L
//     val reverseTimestamps = (minTimestamp to maxTimestamp).reverse.toList
//     val queue = mutable.Queue.from(reverseTimestamps)

//     val reverseClock = () => queue.dequeue()
//     val v = ExactVectorClock[Int,FakeClock](reverseClock)
//     val vNew = (0 until 10).foldLeft(v) { (vDelta, i) => vDelta.put(i) }

//     assert(vNew.timestamps.values.min >= maxTimestamp)
//     assert(vNew.timestamps.values.toSet.size == 10)

//     val sortedByNode = vNew.timestamps.toSeq.sortBy(_._1)
//     val sortedByTimestamp = vNew.timestamps.toSeq.sortBy(_._2)

//     assert(sortedByNode == sortedByTimestamp)
//   }
  
}
