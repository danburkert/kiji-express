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

package org.kiji.express.util

import java.io.InvalidClassException
import java.util.UUID

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificFixed

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * A module with functions that can convert Java values (including Java API Avro values)
 * read from a Kiji table to Scala values (including our Express-specific generic API Avro values)
 * for use in KijiExpress, and vice versa for writing back to a Kiji table.
 *
 * When reading from Kiji, using the specific API, use the method `decodeSpecificFromJava`.
 * When reading from Kiji, using the generic API, use the method `decodeGenericFromJava`.
 * When writing to Kiji, use the method `encodeToJava`.
 *
 * When decoding using the generic API, primitives are converted to Scala primitives, and records
 * are converted to AvroValues. The contents of a record are also AvroValues, even if they
 * represent primitives.  These primitives can be accessed with `asInt`, `asLong`, etc methods.
 * For example: `myRecord("fieldname").asInt` gets an integer field with name `fieldname` from
 * `myRecord`.
 *
 * When decoding using the specific API, primitives are converted to Scala primitives, and records
 * are converted to their corresponding Java classes.
 *
 * Certain AvroValues are used in both the specific and the generic API:
 * <ul>
 *   <li>AvroEnum, which always wraps an enum from Avro.  This is because there is not an easy
 *       Scala equivalent of a Java enum.</li>
 *   <li>AvroFixed, which always wraps a fixed-length byte array from Avro.  This is to distinguish
 *       between a regular byte array, which gets converted to an Array[Byte], and a fixed-length
 *       byte array.</li>
 * <ul>
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] object AvroUtil {

  /**
   * Convert Java types (that came from Avro-deserialized Kiji columns) into corresponding Scala
   * types for usage within KijiExpress.
   *
   * @param value is the value to convert.
   * @return the corresponding value converted to a Scala type.
   */
  private[express] def javaToScala(value: Any): Any = value match {
      // scalastyle:off null
      case null => null
      // scalastyle:on null
      case i: java.lang.Integer => i
      case b: java.lang.Boolean => b
      case l: java.lang.Long => l
      case f: java.lang.Float => f
      case d: java.lang.Double => d
      // bytes
      case bb: java.nio.ByteBuffer => bb.array()
      // raw bytes
      case raw: Array[Byte] => raw
      // string
      case s: java.lang.CharSequence => s.toString
      // array
      case l: java.util.List[_] => {
        l.asScala.toList
      }
      // map (avro maps always have keys of type CharSequence)
      // TODO(EXP-51): revisit conversion of maps between java and scala
      case m: java.util.Map[_, _] => {
        m.asScala.toMap.map(e => e._1.toString -> javaToScala(e._2))
      }
      // fixed
      case f: SpecificFixed => f
      // enum
      case e: java.lang.Enum[_] => e
      // any other type we don't understand
      case _ => {
        val errorMsgFormat = "Read an unrecognized Java object %s with type %s from Kiji that " +
            "could not be converted to a scala type for use with KijiExpress."
        throw new InvalidClassException(errorMsgFormat.format(value, value.getClass))
      }
  }

  /**
   * Converts Scala types back to Java types to write to a Kiji table.
   *
   * @param value is the value written to this column.
   * @return the converted Java type.
   */
  private[express] def scalaToJava(value: Any): Object = value match {
      // scalastyle:off null
      case null => null
      // scalastyle:on null
      case i: Int => i.asInstanceOf[java.lang.Integer]
      case b: Boolean => b.asInstanceOf[java.lang.Boolean]
      case l: Long => l.asInstanceOf[java.lang.Long]
      case f: Float => f.asInstanceOf[java.lang.Float]
      case d: Double => d.asInstanceOf[java.lang.Double]
      case s: String => s
      case bytes: Array[Byte] => bytes
      case l: List[_] => {
        l.map { elem => scalaToJava(elem) }
            .asJava
      }
      // map
      // TODO(EXP-51): revisit conversion of maps between java and scala
      case m: Map[String, _] => m.mapValues(scalaToJava)
      // enum
      case e: java.lang.Enum[_] => e
      // Avro records
      case r: IndexedRecord => r
      // AvroValue
      case _ => throw new InvalidClassException("Trying to write an unrecognized Scala type"
          + " that cannot be converted to a Java type for writing to Kiji: "
          + value.getClass)
  }

  /**
   * Generates a name for a schema consisting of the prefix `schema-` followed by a randomly
   * generated UUID, in which the `-` character has been replaced with `_`.
   *
   * @return the generated schema name.
   */
  private def getRandomSchemaName(): String = {
    "schema_" + UUID.randomUUID().toString().replaceAllLiterally("-", "_")
  }
}
