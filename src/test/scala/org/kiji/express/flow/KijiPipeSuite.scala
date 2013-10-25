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

import com.twitter.scalding._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express._
import org.kiji.express.repl.Implicits._
import org.kiji.express.util.Resources._
import org.kiji.schema.KijiTable
import org.kiji.express.avro.SimpleRecord
import cascading.tuple.Fields
import org.apache.avro.specific.SpecificRecord
import org.kiji.express.util.AvroTupleConversions
import org.kiji.express.flow.FlowTestUtil._
import org.apache.avro.generic.GenericRecord

/**
 * This class simulates running Express jobs in a REPL environment, therefore jobs are not created
 * inside of a subclass of KijiJob; instead we import implicits from the
 * [[org.kiji.express.repl.Implicits]] module.
 */
@RunWith(classOf[JUnitRunner])
class KijiPipeSuite
  extends KijiSuite
  with AvroTupleConversions {

  // Create test Kiji table.
  val uri: String = doAndRelease(makeTestKijiTable(wordCountLayout)) { table: KijiTable =>
    table.getURI().toString()
  }

  test("A KijiPipe can be used to obtain a Scalding job runnable in local or Hadoop mode.") {
    // A job obtained by converting a Cascading Pipe to a KijiPipe, which is then used to obtain
    // a Scalding Job from the pipe.
    def wordCountJob(args: Args): Job = {
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
        .write(Tsv(args("output")))
        .getJob(args)
    }

    val wordCount = JobTest(wordCountJob _)
      .source(KijiInput(uri, "family:column1" -> 'word), wordCountInput)
      .sink(Tsv(outputFile))(validateWordCount)

    wordCount.run.finish
    wordCount.runHadoop.finish
  }

  test("Kiji Avro tuple converters should be in implicit scope.") {
    assert(implicitly[TupleUnpacker[GenericRecord]].getClass ===
      classOf[AvroGenericTupleUnpacker])
    assert(implicitly[TuplePacker[SpecificRecord]].getClass ===
      classOf[AvroSpecificTuplePacker[SpecificRecord]])
    assert(implicitly[TuplePacker[SimpleRecord]].getClass ===
      classOf[AvroSpecificTuplePacker[SimpleRecord]])
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
      .arg("input", inputFile)
      .arg("output", outputFile)
      .source(Tsv(inputFile, new Fields("ls", "s")), simpleInput)
      .sink(Tsv(outputFile))(validateSpecificSimpleRecord)

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
      .arg("input", inputFile)
      .arg("output", outputFile)
      .source(Tsv(inputFile, new Fields("ls", "s")), simpleInput)
      .sink(Tsv(outputFile))(validateGenericSimpleRecord)

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
      .arg("input", inputFile)
      .arg("output", outputFile)
      .source(Tsv(inputFile, new Fields("r")), simpleSpecificRecords)
      .sink(Tsv(outputFile))(validateSpecificSimpleRecord)

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
      .arg("input", inputFile)
      .arg("output", outputFile)
      .source(Tsv(inputFile, new Fields("r")), simpleGenericRecords)
      .sink(Tsv(outputFile))(validateGenericSimpleRecord)

    jobTest.run.finish
    jobTest.runHadoop.finish
  }
}
