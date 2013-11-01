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

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import org.kiji.express.KijiSlice
import org.kiji.schema.KijiColumnName
import org.kiji.schema.filter.KijiColumnFilter

/**
 * Common interface for group-type and map-type input column specifications.
 *
 * Note that implementers of [[org.kiji.express.flow.InputColumnSpec]] are case classes, so they
 * override abstract methods (e.g., ``schema``) with vals.
 */
private[express] trait InputColumnSpec {

  /**
   * Specifies the maximum number of cells (from the most recent) to retrieve from a column.
   *
   * By default, only the most recent cell is retrieved.
   */
  private[express] def maxVersions: Int

  /**
   * Specifies a filter that a cell must pass for this request to retrieve it.
   *
   * If None, a [[org.kiji.schema.filter.KijiColumnFilter]] is not used.
   */
  private[express] def filter: Option[KijiColumnFilter]

  /**
   * Specifies a default value to use for missing cells during a read.
   *
   * If None, rows with missing values are ignored.
   */
  private[express] def default: Option[KijiSlice[_]]

  /**
   * Specifies the maximum number of cells to retrieve from HBase per RPC.
   *
   * If none, paging is disabled.
   */
  private[express] def pageSize: Option[Int]

  /**
   * Specifies the schema type of data to be read from the column.
   */
  private[express] def schema: ReaderSchema

  /**
   * The [[org.kiji.schema.KijiColumnName]] of the column.
   */
  private[express] def columnName: KijiColumnName
}

object InputColumnSpec {

  /**
   * Convenience function for creating a [[org.kiji.express.flow.InputColumnSpec]].  The input
   * spec will be for a group-type column family if the column parameter contains a ':',
   * otherwise the column will be assumed to be a map type.
   *
   * @param column The requested column name.
   * @param schema of data to read from column.  Defaults to default reader schema.
   * @return MapTypeInputColumnSpec with supplied options.
   */
  def apply(
      column: String,
      maxVersions: Int = latest,
      filter: Option[KijiColumnFilter] = None,
      default: Option[KijiSlice[_]] = None,
      pageSize: Option[Int] = None,
      schema: ReaderSchema = Default
  ): InputColumnSpec = {
    column.split(':').toList match {
      case family :: qualifier :: Nil =>
        GroupTypeInputColumnSpec(family, qualifier, schema = schema)
      case family :: Nil =>
        MapTypeInputColumnSpec(family, schema = schema)
      case _ => throw new IllegalArgumentException("column name must contain 'family:qualifier'" +
          " for a group-type, or 'family' for a map-type column.")
    }
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.InputColumnSpec]].  The input
   * spec will be for a group-type column family if the column parameter contains a ':',
   * otherwise the column will be assumed to be a map type.  The column will be read with the
   * schema of the provided specific Avro record.
   *
   * @param column The requested column name.
   * @param avroClass of specific record to read from the column.
   * @return MapTypeInputColumnSpec with supplied options.
   */
  def apply(
      column: String,
      avroClass: Class[_ <: SpecificRecord]
  ): InputColumnSpec = {
    InputColumnSpec(column, schema = Specific(avroClass))
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.InputColumnSpec]].  The input
   * spec will be for a group-type column family if the column parameter contains a ':',
   * otherwise the column will be assumed to be a map type.  The column will be read with the
   * provided generic Avro schema.
   *
   * @param column The requested column name.
   * @param schema of generic Avro type to read from the column.
   * @return MapTypeInputColumnSpec with supplied options.
   */
  def apply(
      column: String,
      schema: Schema
  ): InputColumnSpec = {
    InputColumnSpec(column, schema = Generic(schema))
  }


}

/**
 * Specification for reading from a fully qualified column in a Kiji table.
 *
 * @param family The requested column family name.
 * @param qualifier The requested column qualifier name.
 * @param maxVersions The maximum number of versions to read back (default is only most recent).
 * @param filter Filter to use when reading back cells (default is None).
 * @param default Default KijiSlice to return in case column is empty in row.
 * @param pageSize Maximum number of cells to request from HBase per RPC.
 * @param schema Reader schema specification.  Defaults to the default reader schema.
 */
final case class GroupTypeInputColumnSpec (
    family: String,
    qualifier: String,
    maxVersions: Int = latest,
    filter: Option[KijiColumnFilter] = None,
    default: Option[KijiSlice[_]] = None,
    pageSize: Option[Int] = None,
    schema: ReaderSchema = Default
) extends InputColumnSpec {

  /** Returns name (`family:qualifier`) for requested column as KijiColumnName. */
  override val columnName: KijiColumnName = new KijiColumnName(family, qualifier)
}

object GroupTypeInputColumnSpec {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.GroupTypeInputColumnSpec]] with a
   * specific Avro record type.
   *
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   * @param avroClass of specific record to read from the column.
   * @return MapTypeInputColumnSpec with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      avroClass: Class[_ <: SpecificRecord]
      ): GroupTypeInputColumnSpec = {
    GroupTypeInputColumnSpec(
        family,
        qualifier,
        schema = Specific(avroClass)
    )
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.GroupTypeInputColumnSpec]] with a
   * generic Avro type specified by a [[org.apache.avro.Schema]].
   *
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   * @param schema of generic Avro type to read from the column.
   * @return MapTypeInputColumnSpec with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      schema: Schema
      ): GroupTypeInputColumnSpec = {
    GroupTypeInputColumnSpec(
        family,
        qualifier,
        schema = Generic(schema)
    )
  }
}

/**
 * Specification for reading from a map-type column family in a Kiji table.
 *
 * @param family The requested column family name.
 * @param maxVersions The maximum number of versions to read back (default is only most recent).
 * @param filter Filter to use when reading back cells (default is None).
 * @param default Default KijiSlice to return in case column is empty in row.
 * @param pageSize Maximum number of cells to request from HBase per RPC.
 * @param schema Reader schema specification.  Defaults to the default reader schema.
 */
final case class MapTypeInputColumnSpec(
    family: String,
    maxVersions: Int = latest,
    filter: Option[KijiColumnFilter] = None,
    default: Option[KijiSlice[_]] = None,
    pageSize: Option[Int] = None,
    schema: ReaderSchema = Default
) extends InputColumnSpec {
  require(!family.contains(':'), "family name of map-type column may not contain a ':'.")

  override val columnName: KijiColumnName = new KijiColumnName(family)
}

object MapTypeInputColumnSpec {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.MapTypeInputColumnSpec]] with a
   * specific Avro record type.
   *
   * @param family The requested column family name.
   * @param avroClass of specific record to read from the column.
   * @return MapTypeInputColumnSpec with supplied options.
   */
  def apply(
      family: String,
      avroClass: Class[_ <: SpecificRecord]
  ): MapTypeInputColumnSpec = {
    MapTypeInputColumnSpec(family, schema = Specific(avroClass))
  }

/**
 * Convenience function for creating a [[org.kiji.express.flow.MapTypeInputColumnSpec]] with a
 * generic Avro type specified by a [[org.apache.avro.Schema]].
 *
 * @param family The requested column family name.
 * @param schema of generic Avro type to read from the column.
 * @return MapTypeInputColumnSpec with supplied options.
 */
  def apply(
      family: String,
      schema: Schema
  ): MapTypeInputColumnSpec = {
    MapTypeInputColumnSpec(family, schema = Generic(schema))
  }
}

/**
 * A data type for representing how a cell should be read.  It can be Generic, in which case it
 * represents a generic Avro type (primitive or complex) of the given schema; or Specific,
 * in which case it represents a specific compiled Avro record; or Default, which will be resolved
 * at runtime to a Generic with the default reader schema.
 */
sealed trait ReaderSchema
case class Generic(schema: Schema) extends ReaderSchema
case class Specific(klass: Class[_ <: SpecificRecord]) extends ReaderSchema
case object Default extends ReaderSchema
