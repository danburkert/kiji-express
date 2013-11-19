/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.kiji.express.flow

import org.kiji.express.flow.util.Resources._
import org.kiji.schema.{KijiBufferedWriter, KijiTable, KijiURI, Kiji}
import org.kiji.schema.layout.{KijiTableLayouts, KijiTableLayout}
import scala.util.Random
import org.kiji.express.KijiSuite
import com.twitter.scalding.{Mode, Hdfs, Args, Tsv}
import scala.annotation.tailrec
import org.apache.hadoop.conf.Configuration

/**
 * These tests are meant to test / explore / shed light on the memory characteristics of Express.
 * Accordingly, FakeHBase is not suitable because it keeps all data in heap.  Therefore, it is
 * assumed you have a real (Bento is fine) Kiji instance to run these against.
 *
 * The test data corpus is ~2GB worth of generated data stored into a single Cell in HBase.  This is
 * an absolutely pathological use case for HBase, so don't expect the setup process to be quick.
 */
object MemoryStressSuite extends KijiSuite {
  val table = "memory_stress"
  val uri = KijiURI.newBuilder("kiji://localhost:2181/default/" + table).build()
  val family = "family"
  val column = "c"
  val rowkey = ""

  val cellSize = 1000
  val numCells = 80000

  def setup(): Unit = {
    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      val layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout("layout/memory-stress.json"))
      kiji.createTable(layout.getDesc)

      doAndRelease(kiji.openTable(table)) { table: KijiTable =>
        val eid = table.getEntityId(rowkey) // Empty rowkey
      val rand = new Random
        def value = rand.nextString(cellSize)
        doAndClose(table.getWriterFactory.openBufferedWriter()) { writer: KijiBufferedWriter =>
          Range(0, numCells).foreach(writer.put(eid, family, column, _, value))
        }
      }
    }
  }

  def getConf(): Configuration = {
    doAndRelease(Kiji.Factory.open(uri))(_.getConf)
  }

  def cleanup(): Unit = {
    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      kiji.deleteTable(table)
    }
  }

  @tailrec
  def countStream(count: Int)(stream: Seq[_]): Int = {
    if (count % 100 == 0) println("count:\t" + count)
    if (stream.isEmpty) count
    else countStream(count + 1)(stream.tail)
  }

  def main(args: Array[String]) {

//    cleanup()
    setup()

    Mode.mode = new Hdfs(true, getConf())
    new CountCells(Args("")).run
//    new CountCellsTwice(Args("")).run
//    new FlattenCells(Args("")).run
//    new FlatMapCells(Args("")).run
//    new UnpagedCount(Args("")).run
  }
}

class CountCells(args: Args) extends KijiJob(args) {
  import MemoryStressSuite._
  val column = QualifiedColumnInputSpec("family", "c", all, paging = PagingSpec.Cells(1000))
  val output = Tsv("target/count-cells")
  var count = 0

  KijiInput("kiji://localhost:2181/default/memory_stress", Map(column -> 'strings))
      .read
      .mapTo('strings -> 'count) { x: Seq[FlowCell[_]] => countStream(0)(x) }
      .write(output)
}

class CountCellsTwice(args: Args) extends KijiJob(args) {
  import MemoryStressSuite._
  val column = QualifiedColumnInputSpec("family", "c", all, paging = PagingSpec.Cells(1000))
  val output = Tsv("target/count-cells-twice")
  var count = 0

  KijiInput("kiji://localhost:2181/default/memory_stress", Map(column -> 'strings))
      .read
      .mapTo('strings -> ('a, 'b)) { x: Seq[FlowCell[_]] => countStream(0)(x) -> x.length }
      .write(output)
}

class FlattenCells(args: Args) extends KijiJob(args) {
  val column = QualifiedColumnInputSpec("family", "c", all, paging = PagingSpec.Cells(1000))
  val output = Tsv("target/flatten-cells")
  var count = 0

  KijiInput("kiji://localhost:2181/default/memory_stress", Map(column -> 'strings))
      .read
      .flatten[Seq[FlowCell[_]]]('strings -> 'string)
      .write(output)
}

class FlatMapCells(args: Args) extends KijiJob(args) {
  val column = QualifiedColumnInputSpec("family", "c", all, paging = PagingSpec.Cells(1000))
  val output = Tsv("target/flatmap-cells")
  var count = 0

  KijiInput("kiji://localhost:2181/default/memory_stress", Map(column -> 'strings))
      .read
      .flatMap('strings -> 'string) { xs: Seq[FlowCell[CharSequence]] => xs.map(_.datum) }
      .write(output)
}

class UnpagedCount(args: Args) extends KijiJob(args) {
  val column = QualifiedColumnInputSpec("family", "c", maxVersions = 800)
  val output = Tsv("target/unpaged-count")
  var count = 0

  KijiInput("kiji://localhost:2181/default/memory_stress", Map(column -> 'strings))
      .read
      .map('strings -> ('string, 'type)) { xs: Seq[FlowCell[CharSequence]] => xs.size -> xs.getClass }
      .write(output)
}
