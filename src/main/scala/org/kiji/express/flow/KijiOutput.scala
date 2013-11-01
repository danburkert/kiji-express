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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * Factory methods for constructing [[org.kiji.express.flow.KijiSource]]s that will be used as
 * outputs of a KijiExpress flow. Two basic APIs are provided with differing complexity.
 *
 * Simple:
 * {{{
 *   // Create a KijiOutput that writes to the table named `mytable` putting timestamps in the
 *   // `'timestamps` field and writing the fields `'column1` and `'column2` to the columns
 *   // `info:column1` and `info:column2`.
 *   KijiOutput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       timestampField = 'timestamps,
 *       'column1 -> "info:column1",
 *       'column2 -> "info:column2")
 * }}}
 *
 * Verbose:
 * {{{
 *   // Create a KijiOutput that writes to the table named `mytable` putting timestamps in the
 *   // `'timestamps` field and writing the fields `'column1` and `'column2` to the columns
 *   // `info:column1` and `info:column2`.
 *   KijiOutput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       timestampField = 'timestamps,
 *       columns = Map(
 *           // Enable paging for `info:column1`.
 *           'column1 -> QualifiedColumnRequestOutput("info", "column1"),
 *           'column2 -> QualifiedColumnRequestOutput("info", "column2")))
 * }}}
 *
 * The verbose methods allow you to instantiate explicity
 * [[org.kiji.express.flow.GroupTypeOutputColumnSpec]] and
 * [[org.kiji.express.flow.OutputColumnSpec]] objects.
 * Use the verbose method to specify options for the output columns, e.g.,
 * {{{
 *   // Create a KijiSource that reads from the table named `mytable` reading the columns
 *   // `info:column1` and `info:column2` to the fields `'column1` and `'column2`.
 *   KijiOutput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       timestampField = 'timestamps,
 *       columns = Map(
 *           QualifiedColumnRequestOutput("info", "column1", schemaId=Some(12)) -> 'column1,
 *           QualifiedColumnRequestOutput("info", "column2") -> 'column2)
 * }}}
 */
@ApiAudience.Public
@ApiStability.Experimental
object KijiOutput {
  /**
   * A factory method for instantiating [[org.kiji.express.flow.KijiSource]]s used as sinks. This
   * method permits specifying the full range of output options for each column.  By default,
   * values written will be tagged with the current timestamp version.
   *
   * @param tableUri of Kiji table to write to.
   * @param columns is a mapping specifying what column to write each field value to.
   * @param timestampField containing cell timestamps to use when writing cells. By default,
   *                       the latest timestamp version.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      columns: Map[Symbol, OutputColumnSpec],
      timestampField: Option[Symbol] = None
  ): KijiSource = {
    new KijiSource(
        tableAddress = tableUri,
        timeRange = All,
        timestampField = timestampField,
        loggingInterval = 1000L,
        outputColumns = columns)
  }

  /**
   * A factory method for instantiating [[org.kiji.express.flow.KijiSource]]s used as sinks. This
   * method permits specifying the full range of output options for each column.
   *
   * @param tableUri of Kiji table to write to.
   * @param columns is a mapping specifying what column to write each field value to.
   * @param timestampField containing cell timestamps to use when writing cells.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      columns: Map[Symbol, OutputColumnSpec],
      timestampField: Symbol
  ): KijiSource = {
    KijiOutput(tableUri, columns, Some(timestampField))
  }

  /**
   * A factory method for instantiating a [[org.kiji.express.flow.KijiSource]] to write to
   * group-type columns. Written values will be tagged with the latest timestamp version,
   * and the default reader schema will be used to write values to each column.
   *
   * @param tableUri of Kiji table to write to.
   * @param columns are a series of pairs mapping tuple field names to Kiji column names. When
   *     naming columns, use the format `"family:qualifier"`.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      columns: (Symbol, String)*
  ): KijiSource = {
    val columnMap = columns.toMap.mapValues(column => GroupTypeOutputColumnSpec(column))
    KijiOutput(tableUri, columnMap)
  }

  /**
   * A factory method for instantiating a [[org.kiji.express.flow.KijiSource]] to write to
   * group-type columns. Written values will be tagged with the timestamp contained in the
   * timestampField, and the default reader schema will be used to write values to each column.
   *
   * @param tableUri of Kiji table to write to.
   * @param columns are a series of pairs mapping tuple field names to Kiji column names. When
   *     naming columns, use the format `"family:qualifier"`.
   * @param timestampField contains cell timestamps to write to cells.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      timestampField: Symbol,
      columns: (Symbol, String)*
  ): KijiSource = {
    val columnMap = columns.toMap.mapValues(GroupTypeOutputColumnSpec(_))
    KijiOutput(tableUri, columnMap, Some(timestampField))
  }
}
