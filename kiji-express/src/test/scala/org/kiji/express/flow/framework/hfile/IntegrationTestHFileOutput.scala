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

package org.kiji.express.flow.framework.hfile

import scala.collection.JavaConverters.asScalaIteratorConverter

import com.twitter.scalding.Args
import com.twitter.scalding.IterableSource
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import org.kiji.express.IntegrationUtil._
import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.KijiJob
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.flow.util.Resources._
import org.kiji.express.flow.util.{AvroTypesComplete => ATC}
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.testutil.AbstractKijiIntegrationTest

class IntegrationTestHFileOutput extends AbstractKijiIntegrationTest {

  private var kiji: Kiji = null

  @Before
  def setupTest(): Unit = { kiji = Kiji.Factory.open(getKijiURI()) }

  @After
  def cleanupTest(): Unit = kiji.release()

  @Test
  def testShouldBulkLoadHFiles(): Unit = {
    val conf = getConf()
    val hfileOutput = conf.get("mapred.output.dir")
    FileSystem.get(conf).delete(new Path(hfileOutput), true)

    kiji.createTable(ATC.layout.getDesc)

    doAndRelease(kiji.openTable(ATC.name)) { table =>
      runJob(classOf[HFileOutputInts],
        "--hdfs", "--table-uri", table.getURI.toString, "--hfile-output", hfileOutput)
      bulkLoadHFiles(hfileOutput + "/hfiles", conf, table)

      HFileOutputInts.validate(table)
    }
  }

  @Test
  def testShouldBulkLoadWithReducer(): Unit = {
    val conf = getConf()
    val hfileOutput = conf.get("mapred.output.dir")
    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    kiji.createTable(ATC.layout.getDesc)

    doAndRelease(kiji.openTable(ATC.name)) { table =>
      runJob(classOf[HFileOutputGroupAll],
        "--hdfs", "--table-uri", table.getURI.toString, "--hfile-output", hfileOutput)
      bulkLoadHFiles(hfileOutput + "/hfiles", conf, table)

      HFileOutputGroupAll.validate(table)
    }
  }

  @Test
  def testShouldBulkLoadMultipleTables(): Unit = {
    val conf = getConf()
    val hfileOutput = conf.get("mapred.output.dir")
    val aOutput = hfileOutput + "/a"
    val bOutput = hfileOutput + "/b"

    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    val layoutA = ATC.layout.getDesc
    layoutA.setName("A")
    kiji.createTable(layoutA)

    val layoutB = ATC.layout.getDesc
    layoutB.setName("B")
    kiji.createTable(layoutB)

    doAndRelease(kiji.openTable("A")) { a: KijiTable =>
      doAndRelease(kiji.openTable("B")) { b: KijiTable =>
        runJob(classOf[HFileOutputMultipleTable], "--hdfs",
          "--a", a.getURI.toString, "--b", b.getURI.toString,
          "--a-output", aOutput, "--b-output", bOutput)

        bulkLoadHFiles(aOutput + "/hfiles", conf, a)
        bulkLoadHFiles(bOutput + "/hfiles", conf, b)

        HFileOutputMultipleTable.validateA(a)
        HFileOutputMultipleTable.validateB(b)
      }
    }
  }
}

class HFileOutputInts(args: Args) extends KijiJob(args) {
  val uri = args("table-uri")
  val hfilePath = args("hfile-output")

  IterableSource(HFileOutputInts.inputs, (KijiScheme.EntityIdField, 'int))
    .read
    .write(HFileKijiOutput(uri, hfilePath, 'int -> (ATC.family +":"+ ATC.intColumn)))
}
object HFileOutputInts {
  val inputs: Set[(EntityId, Int)] =
    ((1 to 100).map(int => EntityId(int.toString)) zip (1 to 100)).toSet

  def validate(table: KijiTable) = {
    withKijiTableReader(table) { reader =>
      val request = KijiDataRequest.create(ATC.family, ATC.intColumn)
      doAndClose(reader.getScanner(request)) { scanner =>
        val outputs = for (rowData <- scanner.iterator().asScala)
                      yield (EntityId.fromJavaEntityId(rowData.getEntityId),
                            rowData.getMostRecentValue(ATC.family, ATC.intColumn))
        Assert.assertEquals(inputs, outputs.toSet)
      }
    }
  }
}

class HFileOutputGroupAll(args: Args) extends KijiJob(args) {
  val uri = args("table-uri")
  val hfilePath = args("hfile-output")

  IterableSource(HFileOutputGroupAll.inputs, (KijiScheme.EntityIdField, 'int))
      .read
      .groupAll { _.size }
      .insert('entityId, HFileOutputGroupAll.eid)
      .write(HFileKijiOutput(uri, hfilePath, 'size -> (ATC.family +":"+ ATC.longColumn)))
}
object HFileOutputGroupAll {
  val count = 100
  val inputs = 1 to count
  val eid = EntityId("count")

  def validate(table: KijiTable) = {
    withKijiTableReader(table) { reader =>
      val request = KijiDataRequest.create(ATC.family, ATC.longColumn)
      doAndClose(reader.getScanner(request)) { scanner =>
        val outputs = for (rowData <- scanner.iterator().asScala)
        yield (EntityId.fromJavaEntityId(rowData.getEntityId),
              rowData.getMostRecentValue(ATC.family, ATC.longColumn))
        Assert.assertEquals(List(eid -> count), outputs.toList)
      }
    }
  }
}

class HFileOutputMultipleTable(args: Args) extends KijiJob(args) {
  val aUri = args("a")
  val bUri = args("b")
  val aOutput = args("a-output")
  val bOutput = args("b-output")

  val pipe = IterableSource(HFileOutputMultipleTable.inputs, (KijiScheme.EntityIdField, 'int)).read

  pipe
    .groupAll { _.size }
    .insert('entityId, HFileOutputMultipleTable.eid)
    .write(HFileKijiOutput(bUri, bOutput, 'size -> (ATC.family +":"+ ATC.longColumn)))

  pipe.write(HFileKijiOutput(aUri, aOutput, 'int -> (ATC.family +":"+ ATC.intColumn)))
}
object HFileOutputMultipleTable {
  val eid = EntityId("count")
  val inputs: Set[(EntityId, Int)] =
    ((1 to 10).map(int => EntityId(int.toString)) zip (1 to 10)).toSet

  def validateA(table: KijiTable) = withKijiTableReader(table) { reader =>
    val request = KijiDataRequest.create(ATC.family, ATC.intColumn)
    doAndClose(reader.getScanner(request)) { scanner =>
      val outputs = for (rowData <- scanner.iterator().asScala)
      yield (EntityId.fromJavaEntityId(rowData.getEntityId),
            rowData.getMostRecentValue(ATC.family, ATC.intColumn))
      Assert.assertEquals(inputs, outputs.toSet)
    }
  }
  def validateB(table: KijiTable) = withKijiTableReader(table) { reader =>
    val request = KijiDataRequest.create(ATC.family, ATC.longColumn)
    doAndClose(reader.getScanner(request)) { scanner =>
      val outputs = for (rowData <- scanner.iterator().asScala)
      yield (EntityId.fromJavaEntityId(rowData.getEntityId),
            rowData.getMostRecentValue(ATC.family, ATC.longColumn))
      Assert.assertEquals(List(eid -> inputs.size), outputs.toList)
    }
  }
}

