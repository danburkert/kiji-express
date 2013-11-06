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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiInvalidNameException
import org.kiji.express.util.AvroUtil
import org.kiji.express.SchemaSpec
import org.kiji.express.SchemaSpec.Specific
import org.kiji.express.SchemaSpec.Generic
import org.kiji.express.SchemaSpec.Writer

/**
 * Interface for all column output request specification objects.
 *
 * Note that the subclasses of ColumnRequestOutput are case classes, and so they override
 * ColumnRequestOutput's abstract methods (e.g., schema) with vals.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
trait ColumnRequestOutput {

  /**
   * Family which this [[org.kiji.express.flow.ColumnRequestOutput]] belongs to.
   *
   * @return family name of output column.
   */
  def family: String

  /**
   * [[org.kiji.schema.KijiColumnName]] of this [[org.kiji.express.flow.ColumnRequestOutput]].
   *
   *  @return the name of the column this ColumnRequest specifies.
   */
  def columnName: KijiColumnName

  /**
   * Specifies the schema of data to be written to the column.
   * @return the schema specification of data written to the column.
   */
  def schemaSpec: SchemaSpec

  /**
   * Make a best effort attempt to encode a provided value to a type that will be compatible with
   * the column.  If no such conversion can be made, the original value will be returned.
   */
  private[express] def encode: Any => Any = {
    schemaSpec.schema.map(AvroUtil.avroEncoder).getOrElse(identity)
  }
}

/**
 * Specification for writing to a Kiji column.
 *
 * @param family of the output column.
 * @param qualifier of the output column.
 * @param schemaSpec The schema specification with which to write values. By default uses
 *                   [[org.kiji.express.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class QualifiedColumnRequestOutput(
    family: String,
    qualifier: String,
    schemaSpec: SchemaSpec = Writer
) extends ColumnRequestOutput {
  @transient override lazy val columnName: KijiColumnName = new KijiColumnName(family, qualifier)
}

/**
 * Provides factory functions for [[org.kiji.express.flow.QualifiedColumnRequestOutput]].
 */
@ApiAudience.Public
@ApiStability.Experimental
object QualifiedColumnRequestOutput {
  /**
   * Factory function for creating a [[org.kiji.express.flow.QualifiedColumnRequestOutput]] with a
   * generic Avro writer schema.
   *
   * @param family of the output column.
   * @param qualifier of the output column.
   * @param schema with which to write data.
   */
  def apply(
    family: String,
    qualifier: String,
    schema: Schema
  ): QualifiedColumnRequestOutput = {
    QualifiedColumnRequestOutput(family, qualifier, Generic(schema))
  }

  /**
   * Factory function for creating a [[org.kiji.express.flow.QualifiedColumnRequestOutput]] with a
   * specific Avro record writer schema.
   *
   * @param family of the output column.
   * @param qualifier of the output column.
   * @param specificClass of Avro record with which to write.
   */
  def apply(
    family: String,
    qualifier: String,
    specificClass: Class[_ <: SpecificRecord]
  ): QualifiedColumnRequestOutput = {
    QualifiedColumnRequestOutput(family, qualifier, Specific(specificClass))
  }

  /**
   * Factory function for creating a [[org.kiji.express.flow.QualifiedColumnRequestOutput]].
   * This constructor takes a column string which must contain the column family and qualifier
   * in the form 'family:qualifier'.
   *
   * @param column The output family and column in format 'family:column'.
   * @param schemaSpec specification with which to write data.
   */
  def apply(
      column: String,
      schemaSpec: SchemaSpec
  ): QualifiedColumnRequestOutput = {
    column.split(':').toList match {
      case family :: qualifier :: Nil => QualifiedColumnRequestOutput(family, qualifier, schemaSpec)
      case _ => throw new IllegalArgumentException(
          "Must specify column to GroupTypeOutputColumnSpec in the format 'family:qualifier'.")
    }
  }

  /**
   * Factory function for creating a [[org.kiji.express.flow.QualifiedColumnRequestOutput]] with
   * a generic Avro writer schema. This constructor takes a column string which must contain the
   * column family and qualifier in the form 'family:qualifier'.
   *
   * @param column The output family and column in format 'family:column'.
   * @param schema with which to write data.
   */
  def apply(
      column: String,
      schema: Schema
  ): QualifiedColumnRequestOutput = {
    QualifiedColumnRequestOutput(column, Generic(schema))
  }

  /**
   * Factory function for creating a [[org.kiji.express.flow.QualifiedColumnRequestOutput]] with
   * a generic Avro writer schema. This constructor takes a column string which must contain the
   * column family and qualifier in the form 'family:qualifier'.
   *
   * @param column The output family and column in format 'family:column'.
   * @param specificClass of Avro record with which to write.
   */
  def apply(
    column: String,
    specificClass: Class[_ <: SpecificRecord]
  ): QualifiedColumnRequestOutput = {
    QualifiedColumnRequestOutput(column, Specific(specificClass))
  }

  /**
   * Factory function for creating a [[org.kiji.express.flow.QualifiedColumnRequestOutput]] with
   * the [[org.kiji.express.SchemaSpec.Writer]] schema spec. This constructor takes a column string
   which must contain the column family and qualifier in the form 'family:qualifier'.
   *
   * @param column The output family and column in format 'family:column'.
   */
  def apply(
    column: String
  ): QualifiedColumnRequestOutput = {
    QualifiedColumnRequestOutput(column, Writer)
  }
}

/**
 * Specification for writing to a Kiji column family.
 *
 * @param family of the output column.
 * @param qualifierSelector The field in the Express flow indicating the qualifier of the
 *                          output column.
 * @param schemaSpec The schema spec to use for writing data. By default uses
 *                   [[org.kiji.express.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class ColumnFamilyRequestOutput(
    family: String,
    qualifierSelector: Symbol,
    schemaSpec: SchemaSpec = Writer
) extends ColumnRequestOutput {
  if (family.contains(':')) {
    throw new KijiInvalidNameException("Cannot have a ':' in family name for column family request")
  }
  @transient override lazy val columnName: KijiColumnName = new KijiColumnName(family)
}

/**
 * Provides factory functions for [[org.kiji.express.flow.ColumnFamilyRequestOutput]].
 */
@ApiAudience.Public
@ApiStability.Experimental
object ColumnFamilyRequestOutput {
  /**
   * Factory function for creating a [[org.kiji.express.flow.ColumnFamilyRequestOutput]] with a
   * generic Avro writer schema.
   *
   * @param family of the output column.
   * @param qualifierSelector The field in the Express flow indicating the qualifier of the
   *                          output column.
   * @param schema The schema to use for writing values.
   */
  def apply(
      family: String,
      qualifierSelector: Symbol,
      schema: Schema
  ): ColumnFamilyRequestOutput = {
    ColumnFamilyRequestOutput(family, qualifierSelector, Generic(schema))
  }

  /**
   * Factory function for creating a [[org.kiji.express.flow.ColumnFamilyRequestOutput]] with a
   * specific Avro record writer schema.
   *
   * @param family of the output column.
   * @param qualifierSelector The field in the Express flow indicating the qualifier of the
   *                          output column.
   * @param specificClass The specific record class to use for writes.
   */
  def apply(
    family: String,
    qualifierSelector: Symbol,
    specificClass: Class[_ <: SpecificRecord]
  ): ColumnFamilyRequestOutput = {
    ColumnFamilyRequestOutput(family, qualifierSelector, Specific(specificClass))
  }
}
