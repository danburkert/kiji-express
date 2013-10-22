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

import com.twitter.scalding._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express._
import org.kiji.express.repl.Implicits._
import org.kiji.express.util.Resources._
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.express.avro.SimpleRecord
import cascading.tuple.Fields
import org.apache.avro.generic.{GenericRecordBuilder, GenericRecord}
import org.apache.avro.specific.SpecificRecord
import org.kiji.express.util.AvroTupleConversions

/**
 * This class is for testing KijiPipe in a REPL-like environment.
 */
@RunWith(classOf[JUnitRunner])
class KijiPipeSuite
  extends KijiSuite
  with AvroTupleConversions {
  /** Table layout to use for tests. */
  @transient
  val layout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Input tuples to use for word count tests. */
  val wordCountInput: List[(EntityId, KijiSlice[String])] = List(
    ( EntityId("row01"), slice("family:column1", (1L, "hello")) ),
    ( EntityId("row02"), slice("family:column1", (2L, "hello")) ),
    ( EntityId("row03"), slice("family:column1", (1L, "world")) ),
    ( EntityId("row04"), slice("family:column1", (3L, "hello")) ))

  /**
   * Validates output from the word count tests.

   * @param outputBuffer containing data that output buffer has in it after the job has been run.
   */
  def validateWordCount(outputBuffer: Buffer[(String, Int)]) {
    val outMap = outputBuffer.toMap

    // Validate that the output is as expected.
    assert(3 === outMap("hello"))
    assert(1 === outMap("world"))
  }

  // Create test Kiji table.
  val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
    table.getURI().toString()
  }

  // A name for a dummy file for test job output.
  val outputFile: String = "outputFile"

  // A job obtained by converting a Cascading Pipe to a KijiPipe, which is then used to obtain
  // a Scalding Job from the pipe.
  def jobToRun(args: Args): Job = {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    KijiInput(uri, "family:column1" -> 'word)
    // Sanitize the word.
    .map('word -> 'cleanword) { words: KijiSlice[String] =>
      words.getFirstValue()
          .toString()
          .toLowerCase()
    }
    // Count the occurrences of each word.
    .groupBy('cleanword) { occurences => occurences.size }
    // Write the result to a file.
    .write(Tsv("outputFile"))
    .getJob(args)
  }

  /** The job tester we'll use to run the test job in either local or hadoop mode. */
  @transient
  val myJobTest = JobTest(jobToRun _)
    .source(KijiInput(uri, "family:column1" -> 'word), wordCountInput)
    .sink(Tsv("outputFile"))(validateWordCount)

  test("A KijiPipe can be used to obtain a Scalding job that is run in local mode.") {
    myJobTest.run.finish
  }

  test("A KijiPipe can be used to obtain a Scalding job that is run with Hadoop.") {
    myJobTest.runHadoop.finish
  }

  test("Kiji Avro tuple converters should be in implicit scope.") {
    assert(implicitly[TupleUnpacker[GenericRecord]].getClass ===
      classOf[AvroGenericTupleUnpacker])
    assert(implicitly[TuplePacker[SpecificRecord]].getClass ===
      classOf[AvroSpecificTuplePacker[SpecificRecord]])
    assert(implicitly[TuplePacker[SimpleRecord]].getClass ===
      classOf[AvroSpecificTuplePacker[SimpleRecord]])
  }

  val simpleSchema = SimpleRecord.getClassSchema

  val simpleInput: List[(Long, String)] = List(
    (1, "foobar"),
    (2, "shoe"),
    (3, "random"),
    (99, "baloons"),
    (356, "sumerians"))

  val simpleSpecificRecords: List[SimpleRecord] = simpleInput.map { t =>
    SimpleRecord.newBuilder.setL(t._1).setS(t._2).build()
  }

  val simpleGenericRecords: List[GenericRecord] = simpleInput.map { t =>
    new GenericRecordBuilder(simpleSchema).set("l", t._1).set("s", t._2).build()
  }

  def validateSpecificSimpleRecord(outputs: Buffer[(Long, String, String, SimpleRecord)]) {
    outputs.foreach { t =>
      val (l, s, o, r) = t
      assert(simpleSpecificRecords.contains(r))
      assert(r.getL === l)
      assert(r.getS === s)
      assert(r.getO === o)
    }
  }

  def validateGenericSimpleRecord(outputs: Buffer[(Long, String, String, GenericRecord)]) {
    outputs.foreach { t =>
      val (l, s, o, r) = t
      assert(simpleGenericRecords.contains(r))
      assert(r.get("l") === l)
      assert(r.get("s") === s)
      assert(r.get("o") === o)
    }
  }

  test("A KijiPipe can pack fields into a specific Avro record.") {
    def packSpecificRecordJob(args: Args): Job = {
      Tsv(args("input"), new Fields("ls", "s"))
        .map('ls -> 'l) { ls: String => ls.toLong }
        .pack[SimpleRecord](('l, 's) -> 'r)
        .map('r -> 'o) { r: SimpleRecord => r.getO }
        .project('l, 's, 'o, 'r)
        .write(Tsv(args("output")))
        .getJob(args)
    }

    val jobTest = JobTest(packSpecificRecordJob _)
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(Tsv("inputFile", new Fields("ls", "s")), simpleInput)
      .sink(Tsv("outputFile"))(validateSpecificSimpleRecord)

    jobTest.run.finish
    jobTest.runHadoop.finish
  }

  test("A KijiPipe can pack fields into a generic Avro record.") {
    def packGenericRecordJob(args: Args): Job = {
      Tsv(args("input"), new Fields("ls", "s"))
        .read
        .map('ls -> 'l) { ls: String => ls.toLong }
        .packGenericRecord(('l, 's) -> 'r)(simpleSchema)
        .map('r -> 'o) { r: GenericRecord => r.get("o") }
        .project('l, 's, 'o, 'r)
        .write(Tsv(args("output")))
        .getJob(args)
    }

    val jobTest = JobTest(packGenericRecordJob _)
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(Tsv("inputFile", new Fields("ls", "s")), simpleInput)
      .sink(Tsv("outputFile"))(validateGenericSimpleRecord)

    jobTest.run.finish
    jobTest.runHadoop.finish
  }

  test("A KijiPipe can unpack a specific Avro record into fields.") {
    def unpackSpecificRecordJob(args: Args): Job = {
      Tsv(args("input"), new Fields("r"))
        .read
        .unpack[SimpleRecord]('r -> ('l, 's, 'o))
        .project('l, 's, 'o, 'r)
        .write(Tsv(args("output")))
        .getJob(args)
    }

    val jobTest = JobTest(unpackSpecificRecordJob _)
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(Tsv("inputFile", new Fields("r")), simpleSpecificRecords)
      .sink(Tsv("outputFile"))(validateSpecificSimpleRecord)

    jobTest.run.finish
    jobTest.runHadoop.finish
  }

  test("A KijiPipe can unpack a generic Avro record into fields.") {
    def unpackRecordJob(args: Args): Job = {
      Tsv(args("input"), new Fields("r"))
        .read
        .unpack[GenericRecord]('r -> ('l, 's, 'o))
        .project('l, 's, 'o, 'r)
        .write(Tsv(args("output")))
        .getJob(args)
    }

    val jobTest = JobTest(unpackRecordJob _)
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(Tsv("inputFile", new Fields("r")), simpleGenericRecords)
      .sink(Tsv("outputFile"))(validateGenericSimpleRecord)

    jobTest.run.finish
    jobTest.runHadoop.finish
  }
}
