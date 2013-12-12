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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.express.flow.framework.KijiScheme
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.KijiURI

@RunWith(classOf[JUnitRunner])
class FlowModuleSuite extends FunSuite {
  val tableURI = "kiji://.env/default/table"

  test("Flow module forbids creating an input map-type column with a qualifier in the column "
      + "name.") {
    intercept[KijiInvalidNameException] {
      ColumnFamilyInputSpec("info:word")
    }
  }

  test("Flow module forbids creating an output map-type column with a qualifier in the column "
      + "name.") {
    intercept[IllegalArgumentException] {
      ColumnFamilyOutputSpec.builder
          .withFamily("info:word")
          .withQualifierSelector('foo)
          .build
    }
  }

  test("Flow module permits creating an output map-type column specifying the qualifier field") {
    ColumnFamilyOutputSpec.builder
        .withFamily("searches")
        .withQualifierSelector('terms)
        .build
  }

  test("Flow module permits specifying a qualifier regex on ColumnFamilyInputSpec.") {
    val colReq = ColumnFamilyInputSpec.builder
        .withFamily("search")
        .withFilterSpec(ColumnFilterSpec.RegexQualifierFilterSpec(""".*\.com"""))
        .build

    // TODO: Test it filters keyvalues correctly.
    assert(colReq.filterSpec.isInstanceOf[ColumnFilterSpec.RegexQualifierFilterSpec])
  }

  test("Flow module permits specifying a qualifier regex (with filter) on ColumnFamilyInputSpec.") {
    val colReq = ColumnFamilyInputSpec.builder
        .withFamily("search")
        .withFilterSpec(ColumnFilterSpec.RegexQualifierFilterSpec(""".*\.com"""))
        .build

    // TODO: Test it filters keyvalues correctly.
    assert(colReq.filterSpec.isInstanceOf[ColumnFilterSpec.RegexQualifierFilterSpec])
  }

  test("Flow module permits specifying versions on map-type columns without qualifier regex.") {
    val colReq = ColumnFamilyInputSpec("search", maxVersions=2)
    assert(2 === colReq.maxVersions)
  }

  test("Flow module permits specifying versions on a group-type column.") {
    val colReq = QualifiedColumnInputSpec.builder
        .withColumn("info", "word")
        .withMaxVersions(2)
        .build
    assert(2 === colReq.maxVersions)
  }

  test("Flow module uses default versions of 1 for all ColumnInputSpecs.") {
    val groupReq = QualifiedColumnInputSpec("info", "word")
    val mapReq = ColumnFamilyInputSpec("searches")

    assert(1 === groupReq.maxVersions)
    assert(1 === mapReq.maxVersions)
  }

  test("Flow module permits creating inputs and outputs with no mappings.") {
    val input: KijiSource = KijiInput(tableURI, columns = Map[ColumnInputSpec, Symbol]())
    val output: KijiSource = KijiOutput.builder
        .withTableURI(tableURI)
        .withColumnSpecs(Map[Symbol, ColumnOutputSpec]())
        .build

    assert(input.inputColumns.isEmpty)
    assert(input.outputColumns.isEmpty)
    assert(output.inputColumns.isEmpty)
    assert(output.outputColumns.isEmpty)
  }

  test("Flow module permits creating KijiSources as inputs with default options.") {
    val input: KijiSource = KijiInput(tableURI, "info:word" -> 'word)
    val expectedScheme = new KijiScheme(
        tableUri = KijiURI.newBuilder(tableURI).build(),
        timeRange = All,
        timestampField = None,
        icolumns = Map("word" -> QualifiedColumnInputSpec("info", "word")))

    assert(expectedScheme === input.hdfsScheme)
  }

  test("Flow module permits specifying timerange for KijiInput.") {
    val input = KijiInput(tableURI, timeRange=Between(0L,40L), columns="info:word" -> 'word)
    val expectedScheme = new KijiScheme(
        KijiURI.newBuilder(tableURI).build(),
        Between(0L, 40L),
        None,
        Map("word" -> QualifiedColumnInputSpec("info", "word")))

    assert(expectedScheme === input.hdfsScheme)
  }

  test("Flow module permits creating KijiSources with multiple columns.") {
    val input: KijiSource = KijiInput(tableURI, "info:word" -> 'word, "info:title" -> 'title)
    val expectedScheme: KijiScheme = {
      new KijiScheme(
          KijiURI.newBuilder(tableURI).build(),
          All,
          None,
          Map(
              "word" -> QualifiedColumnInputSpec("info", "word"),
              "title" -> QualifiedColumnInputSpec("info", "title")))
    }

    assert(expectedScheme === input.hdfsScheme)
  }

  test("Flow module permits specifying options for a column.") {
    KijiInput(
        tableURI,
        Map(QualifiedColumnInputSpec("info", "word") -> 'word)
    )

    KijiInput(
        tableURI,
        Map(QualifiedColumnInputSpec.builder
            .withColumn("info", "word")
            .withMaxVersions(1)
            .build -> 'word)
    )

    KijiInput(
        tableURI,
        Map(
            ColumnFamilyInputSpec(
                "searches",
                maxVersions = 1,
                filterSpec = ColumnFilterSpec.RegexQualifierFilterSpec(".*")
            ) -> 'word
        )
    )
  }

  test("Flow module permits specifying different options for different columns.") {
    KijiInput(
        tableURI,
        Map(
            QualifiedColumnInputSpec.builder
                .withColumn("info", "word")
                .withMaxVersions(1)
                .build -> 'word,
            QualifiedColumnInputSpec.builder
                .withColumn("info", "title")
                .withMaxVersions(2)
                .build -> 'title))
  }

  test("Flow module permits creating KijiSource with the default timestamp field") {
    val output: KijiSource = KijiOutput.builder
        .withTableURI(tableURI)
        .withColumns('words -> "info:words")
        .build
    val expectedScheme: KijiScheme = new KijiScheme(
        tableUri = KijiURI.newBuilder(tableURI).build(),
        timeRange = All,
        timestampField = None,
        ocolumns = Map("words" -> QualifiedColumnOutputSpec.builder
            .withColumn("info", "words")
            .build))
    assert(expectedScheme === output.hdfsScheme)
  }

  test("Flow module permits creating KijiSource with a timestamp field") {
    val output: KijiSource = KijiOutput.builder
        .withTableURI(tableURI)
        .withTimestampField('time)
        .withColumns('words -> "info:words")
        .build
    val expectedScheme: KijiScheme = new KijiScheme(
        tableUri = KijiURI.newBuilder(tableURI).build(),
        timeRange = All,
        timestampField = Some('time),
        ocolumns = Map("words" -> QualifiedColumnOutputSpec.builder
            .withColumn("info", "words")
            .build))
    assert(expectedScheme === output.hdfsScheme)
  }
}
