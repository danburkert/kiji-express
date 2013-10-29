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

import scala.collection.mutable.Buffer

import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.{KijiColumnName, KijiTable, KijiURI}
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType
import org.kiji.schema.layout.KijiTableLayout
import org.apache.avro.generic.GenericRecord

@RunWith(classOf[JUnitRunner])
class KijiJobSuite extends KijiSuite {
  val avroLayout: KijiTableLayout = layout("layout/avro-types.json")
  val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
    table.getURI().toString()
  }

  val column4Schema = avroLayout.getCellSpec(new KijiColumnName("family:column4")).getAvroSchema


  test("A KijiJob can run with a pipe that uses packGenericRecord.") {
    val packingInput: List[(String, String)] = List(
      ( "0", "1 eid1 word1" ),
      ( "1", "3 eid1 word2" ),
      ( "2", "5 eid2 word3" ),
      ( "3", "7 eid2 word4" ))

    def validatePacking(outputBuffer: Buffer[(String, GenericRecord)]) {
      outputBuffer.foreach { t =>
        val (string, record) = t
        assert(string == record.get("contained_string"))
      }
    }


    class PackTupleJob(args: Args) extends KijiJob(args) {
      TextLine(args("input")).read
        .rename('line -> 'contained_string)
        .project('contained_string)
        .packGenericRecord('contained_string -> 'r)(column4Schema)
        .write(Tsv(args("output")))
    }

    val jobTest = JobTest(new PackTupleJob(_))
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(TextLine("inputFile"), packingInput)
      .sink(Tsv("outputFile"))(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob can run with a pipe that uses unpacks a specific record.") {
    val specificRecord = new HashSpec(HashType.MD5, 13, true)

    val unpackingInput: List[(EntityId, KijiSlice[HashSpec])] =
      List((EntityId("row01"), slice("family:column3", (10L, specificRecord))))

    def validatePacking(outputBuffer: Buffer[(HashSpec, HashType, Int, Boolean)]) {
      outputBuffer.foreach { t =>
        val (record, hashType, hashSize, suppress) = t
        assert(record.getHashType === hashType)
        assert(record.getHashSize === hashSize)
        assert(record.getSuppressKeyMaterialization === suppress)
      }
    }

    class UnpackTupleJob(args: Args) extends KijiJob(args) {
      KijiInput(args("input"), "family:column3" -> 'slice)
        .mapTo('slice -> 'record) { slice: KijiSlice[HashSpec] => slice.getFirstValue }
        .unpack[HashSpec]('record -> ('hashType, 'hashSize, 'suppressKeyMaterialization))
        .write(Tsv(args("output")))
    }

    val jobTest = JobTest(new UnpackTupleJob(_))
      .arg("input", uri)
      .arg("output", "outputFile")
      .source(KijiInput(uri, Map(Column("family:column3") -> 'slice)), unpackingInput)
      .sink(Tsv("outputFile"))(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
//    jobTest.runHadoop.finish
  }

  test("A KijiJob implicitly converts KijiSources to KijiPipes so you can call Kiji methods.") {
    // This class won't actually run because slice will contain a KijiSlice which can't be
    // unpacked.  This tests only tests that this compiles.  Eventually we may have methods
    // on KijiPipe that we need to use directly after KijiInput (for example, we may decide to
    // return only the first value in the slice if only 1 value is requested).
    class UnpackTupleJob(args: Args) extends KijiJob(args) {
      KijiInput(args("input"), "family:column3" -> 'slice)
        .unpackAvro('slice -> ('hashtype, 'hashsize, 'suppress))
        .project('hashtype, 'hashsize, 'suppress)
        .write(Tsv(args("output")))
    }
  }

  test("A KijiJob is not run if the Kiji instance in the output doesn't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .map ('line -> 'entityId) { line: String => EntityId(line) }
        .write(KijiOutput(args("output"), 'line -> "family:column1"))
    }

    val nonexistentInstanceURI: String = KijiURI.newBuilder(uri)
      .withInstanceName("nonexistent_instance")
      .build()
      .toString

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
      .arg("input", "inputFile")
      .arg("output", nonexistentInstanceURI)
      .source(TextLine("inputFile"), basicInput)
      .sink(KijiOutput(nonexistentInstanceURI, 'line -> "family:column1"))(validateBasicJob)

    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }
    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_instance"))
  }

  test("A KijiJob is not run if the Kiji table in the output doesn't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .write(KijiOutput(args("output"), 'line -> "family:column1"))
    }

    val nonexistentTableURI: String = KijiURI.newBuilder(uri)
      .withTableName("nonexistent_table")
      .build()
      .toString

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
      .arg("input", "inputFile")
      .arg("output", nonexistentTableURI)
      .source(TextLine("inputFile"), basicInput)
      .sink(KijiOutput(nonexistentTableURI, 'line -> "family:column1"))(validateBasicJob)

    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_table"))
  }

  test("A KijiJob is not run if any of the columns don't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .write(KijiOutput(args("output"), 'line -> "family:nonexistent_column"))
    }

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
      .arg("input", "inputFile")
      .arg("output", uri)
      .source(TextLine("inputFile"), basicInput)
      .sink(KijiOutput(uri, 'line -> "family:nonexistent_column"))(validateBasicJob)

    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_column"))
  }
}