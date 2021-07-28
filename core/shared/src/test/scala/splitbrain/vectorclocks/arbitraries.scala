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
import java.util.concurrent.atomic.AtomicLong

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Cogen
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import ExactVectorClock.Timestamp

import scala.annotation.nowarn
import scala.collection.immutable.HashMap

object arbitraries {

  private val minTs = 100L

  private def genTimestamps[Node: Arbitrary] =
    Gen.mapOf[Node, Long] {
      for {
        timestamp <- Gen.choose(0L, minTs)
        node      <- arbitrary[Node]
      } yield node -> timestamp
    }

  implicit val cogenUUID: Cogen[UUID] =
    Cogen((seed: Seed, id: UUID) => Cogen.perturbPair(seed, (id.getLeastSignificantBits, id.getMostSignificantBits)))

  implicit def cogenVClock[Node: Cogen: Ordering]: Cogen[ExactVectorClock[Node]] =
    Cogen { (seed: Seed, v: ExactVectorClock[Node]) =>
      {
        val tsMap: Map[Node, Timestamp] = v.timestamps
        Cogen.perturb(seed, tsMap)
      }
    }

  @nowarn("cat=unused-imports")
  implicit def arbitraryVClock[Node: Arbitrary]: Arbitrary[ExactVectorClock[Node]] = {
    import scala.collection.compat._
    Arbitrary(
      genTimestamps[Node].map(ts => ExactVectorClock(timestamps = HashMap.from(ts), counter = new AtomicLong(minTs))))
  }
}
