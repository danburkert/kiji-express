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

import com.twitter.scalding.{TextLine, JobTest, Args, Tsv}

import org.apache.avro.generic.{GenericRecordBuilder, GenericRecord}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.flow.FlowTestUtil._
import org.kiji.express.util.Resources._
import org.kiji.express.{EntityId, KijiSlice, KijiSuite}
import org.kiji.schema.avro.{HashType, HashSpec}
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.{KijiColumnName, KijiTable}
import scala.collection.mutable
import org.apache.avro.Schema
import cascading.tuple.Fields

/**
 * This class simulates running Express jobs as part of a [[org.kiji.express.flow.KijiJob]],
 * therefore all tests are wrapped in a subclass of [[org.kiji.express.flow.KijiJob]].
 */
@RunWith(classOf[JUnitRunner])
class KijiJobSuite extends KijiSuite {

  val avroLayout: KijiTableLayout = layout("layout/avro-types.json")
  val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
    table.getURI.toString
  }

  def validateBasicJob(outputBuffer: mutable.Buffer[String]) { /** Nothing to validate. */ }

  /** module for holding inputs for running tests on the avro-type table. */
  object inputs {

    val strings: List[String] = List(
      "the quick brown fox jumped over the lazy dog.\n",
      "semper ubi sub ubi !@#$%^&*()_+",
      "Kiji Wiji Hiji Piji Miji",
      "in a world lit only by fireflys,",
      "words from the end of a techno song")

    val column1: List[Array[Byte]] = strings.map(_.take(16)).map(_.getBytes("UTF8"))
    val column2: List[Array[Byte]] = strings.map(_.getBytes("UTF8"))

    val column3Fields: List[(HashType, Int, Boolean)] =
      (List.fill(5)(HashType.MD5), Range(0, 4), List(true, false, true, false, true)).zipped.toList
    val column3Records: List[HashSpec] = column3Fields.map(t => new HashSpec(t._1, t._2, t._3))

    val column4Schema: Schema =
      avroLayout.getCellSpec(new KijiColumnName("family:column4")).getAvroSchema
    val column4Records: List[GenericRecord] = strings.map { s: String =>
      new GenericRecordBuilder(column4Schema)
        .set("contained_string", s)
        .build()
    }
  }

  test("A KijiJob can write to a KijiTable String column.") {
    class WriteWordsJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .map('line -> 'entityId) {w: String => EntityId(w)}
        .write(KijiOutput(args("table-uri"), 'line -> "family:column1"))
    }

    val jobTest = JobTest(new WriteWordsJob(_))
      .arg("input", inputFile)
      .arg("table-uri", uri)
      .source(TextLine(inputFile), inputs.strings)
      .sink(KijiOutput(uri, 'column1 -> "family:column1"))(validateBasicJob)

    jobTest.run.finish
    jobTest.runHadoop.finish
  }
}