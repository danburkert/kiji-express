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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

@RunWith(classOf[JUnitRunner])
class ColumnRequestSuite extends FunSuite {
  def filter: KijiColumnFilter = new RegexQualifierColumnFilter(".*")
  val colFamily = "myfamily"
  val colQualifier = "myqualifier"
  val qualifierSelector = 'qualifierSym

  // TODO(CHOP-37): Test with different filters once the new method of specifying filters
  // correctly implements the .equals() and hashCode() methods.
  // Should be able to change the following line to:
  // def filter = new RegexQualifierColumnFilter(".*")
  val maxVersions = 2

  val colWithOptions: GroupTypeInputColumnSpec = GroupTypeInputColumnSpec(
      family = colFamily,
      qualifier = colQualifier,
      maxVersions = maxVersions,
      filter = Some(filter)
  )

  test("Fields of a ColumnFamilyRequestInput are the same as those it is constructed with.") {
    val col: MapTypeInputColumnSpec = new MapTypeInputColumnSpec(family = colFamily)
    assert(colFamily === col.family)
  }

  test("InputColumnSpec factory method creates ColumnFamilyRequestInput.") {
    val col = InputColumnSpec(colFamily)
    assert(col.isInstanceOf[MapTypeInputColumnSpec])
    assert(colFamily === col.asInstanceOf[MapTypeInputColumnSpec].family)
  }

  test("Fields of a MapTypeOutputColumnSpec are the same as those it is constructed with.") {
    val col: MapTypeOutputColumnSpec = new MapTypeOutputColumnSpec(
        family = colFamily,
        qualifierSelector = qualifierSelector
    )
    assert(colFamily === col.family)
    assert(qualifierSelector === col.qualifierSelector)
  }

  test("OutputColumnSpec factory method creates MapTypeOutputColumnSpec.") {
    val col = MapTypeOutputColumnSpec(colFamily, qualifierSelector)
    assert(col.isInstanceOf[MapTypeOutputColumnSpec])
    assert(colFamily === col.asInstanceOf[MapTypeOutputColumnSpec].family)
  }

  test("Fields of a QualifiedInputColumnSpec are the same as those it is constructed with.") {
    val col: GroupTypeInputColumnSpec = new GroupTypeInputColumnSpec(colFamily, colQualifier)

    assert(colFamily === col.family)
    assert(colQualifier === col.qualifier)
  }

  test("InputColumnSpec factory method creates QualifiedInputColumnSpec.") {
    val col = GroupTypeInputColumnSpec(colFamily, colQualifier)
    assert(col.isInstanceOf[GroupTypeInputColumnSpec])
    assert(colFamily === col.asInstanceOf[GroupTypeInputColumnSpec].family)
    assert(colQualifier === col.asInstanceOf[GroupTypeInputColumnSpec].qualifier)
  }

  test("Fields of a QualifiedOutputColumnSpec are the same as those it is constructed with.") {
    val col: GroupTypeOutputColumnSpec =
        new GroupTypeOutputColumnSpec(colFamily, colQualifier)

    assert(colFamily === col.family)
    assert(colQualifier === col.qualifier)
  }

  test("OutputColumnSpec factory method creates QualifiedOutputColumnSpec.") {
    val col = GroupTypeOutputColumnSpec(colFamily, colQualifier)
    assert(col.isInstanceOf[GroupTypeOutputColumnSpec])
    assert(colQualifier === col.asInstanceOf[GroupTypeOutputColumnSpec].qualifier)
    assert(colFamily === col.asInstanceOf[GroupTypeOutputColumnSpec].family)
  }

  test("Two ColumnFamilys with the same parameters are equal and hash to the same value.") {
    val col1 = new MapTypeInputColumnSpec(colFamily)
    val col2 = new MapTypeInputColumnSpec(colFamily)

    assert(col1 === col2)
    assert(col1.hashCode() === col2.hashCode())
  }

  test("Two qualified columns with the same parameters are equal and hash to the same value.") {
    val col1 = new GroupTypeInputColumnSpec(colFamily, colQualifier)
    val col2 = new GroupTypeInputColumnSpec(colFamily, colQualifier)

    assert(col1 === col2)
    assert(col1.hashCode() === col2.hashCode())
  }

  test("A column must be serializable.") {
    // Serialize and deserialize using java ObjectInputStream and ObjectOutputStream.
    val col = new GroupTypeInputColumnSpec(colFamily, colQualifier)
    val bytesOut = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytesOut)
    out.writeObject(col)
    val serializedColumn = bytesOut.toByteArray()
    val bytesIn = new ByteArrayInputStream(serializedColumn)
    val in = new ObjectInputStream(bytesIn)
    val deserializedColumn = in.readObject()

    assert(col === deserializedColumn)
  }

  test("maxVersions is the same as constructed with.") {
    assert(maxVersions == colWithOptions.maxVersions)
  }

  test("Default maxVersions is 1.") {
    assert(1 == GroupTypeInputColumnSpec(colFamily, colQualifier).maxVersions)
  }

  test("Filter is the same as constructed with.") {
    assert(Some(filter) == colWithOptions.filter)
  }

  test("InputColumnSpec with the same maxVersions & filter are equal and hash to the same "
      + "value.") {

    val col2: GroupTypeInputColumnSpec = new GroupTypeInputColumnSpec(
        colFamily, colQualifier,
        filter = Some(filter),
        maxVersions = maxVersions
    )
    assert(col2 === colWithOptions)
    assert(col2.hashCode() === colWithOptions.hashCode())
  }
}
