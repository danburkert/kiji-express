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
import org.kiji.schema.KijiColumnName

/**
 * Common interface for group-type and map-type output column specifications.
 *
 * Note that the subclasses of ColumnRequestOutput are case classes, so they override
 * [[org.kiji.express.flow.OutputColumnSpec]]'s abstract methods (e.g., ``schema``) with vals.
 */
private[express] trait OutputColumnSpec {

  /** Family which this column spec belongs to. */
  def family: String

  /**
   * [[org.kiji.schema.KijiColumnName]] of this [[org.kiji.express.flow.OutputColumnSpec]].
   *
   *  @return the name of the column this ColumnRequest specifies.
   */
  def columnName: KijiColumnName

  /**
   * [[org.apache.avro.Schema]] to be written to this output column.
   * @return
   */
  def schema: Option[Schema]
}

/**
 * Specification for writing to a group-type column in a Kiji table.
 *
 * @param family The group-type family containing the column to write to.
 * @param qualifier The column qualifier to write to.
 * @param schema The schema to write with. If None, the default reader schema is used.
 */
final case class GroupTypeOutputColumnSpec(
    family: String,
    qualifier: String,
    schema: Option[Schema] = None
) extends OutputColumnSpec {
  val columnName: KijiColumnName = new KijiColumnName(family, qualifier)
}

/**
 * Provides convenience factory functions for [[org.kiji.express.flow.GroupTypeOutputColumnSpec]].
 */
object GroupTypeOutputColumnSpec {
  /**
   * Specification for writing to a group-type column in a Kiji table.
   *
   * @param family The group-type family containing the column to write to.
   * @param qualifier The column qualifier to write to.
   * @param schema The schema to write with.
   */
  def apply(
      family: String,
      qualifier: String,
      schema: Schema
  ): GroupTypeOutputColumnSpec = {
    GroupTypeOutputColumnSpec(family, qualifier, Some(schema))
  }

  /**
   * Specification for writing to a group-type column in a Kiji table.
   *
   * @param column The group-type family and column in format 'family:column'.
   * @param schema The schema to write with.
   */
  def apply(
      column: String,
      schema: Option[Schema]
  ): GroupTypeOutputColumnSpec = {
    column.split(':').toList match {
      case family :: qualifier :: Nil => GroupTypeOutputColumnSpec(family, qualifier, schema)
      case _ => throw new IllegalArgumentException(
        "Must specify column to GroupTypeOutputColumnSpec in the format 'family:qualifier'.")
    }
  }

  /**
   * Specification for writing to a group-type column in a Kiji table.
   *
   * @param column The group-type family and column in format 'family:column'.
   */
  def apply(
      column: String
  ): GroupTypeOutputColumnSpec = {
    GroupTypeOutputColumnSpec(column, None)
  }

  /**
   * Specification for writing to a group-type column in a Kiji table.
   *
   * @param column The group-type family and column in format 'family:column'.
   * @param schema The schema to write with.
   */
  def apply(
    column: String,
    schema: Schema
  ): GroupTypeOutputColumnSpec = {
    column.split(':').toList match {
      case family :: qualifier :: Nil => GroupTypeOutputColumnSpec(family, qualifier, Some(schema))
      case _ => throw new IllegalArgumentException(
        "Must specify column to GroupTypeOutputColumnSpec in the format 'family:qualifier'.")
    }
  }
}

/**
 * Specification for writing to a map-type column family in a Kiji table.
 *
 * @param family The map-type family to write to.
 * @param qualifierSelector The field in the Express flow indicating what column to write to.
 * @param schema The schema to use for writes.  If None, the default reader schema is used.
 */
final case class MapTypeOutputColumnSpec(
    family: String,
    qualifierSelector: Symbol,
    schema: Option[Schema] = None
) extends OutputColumnSpec {
  require(!family.contains(':'), "MapTypeOutputSpec may not have family name which contains ':'.")

  val columnName = new KijiColumnName(family)
}

/**
 * Provides convenience factory functions for [[org.kiji.express.flow.MapTypeOutputColumnSpec]].
 */
object MapTypeOutputColumnSpec {
  /**
   * Specification for writing to a map-type column family in a Kiji table.
   *
   * @param family The map-type family to write to.
   * @param qualifierSelector The field in the Express flow indicating what column to write to.
   * @param schema The schema to use for writes.
   */
  def apply(family: String, qualifierSelector: Symbol, schema: Schema) {
    MapTypeOutputColumnSpec(family, qualifierSelector, Some(schema))
  }
}