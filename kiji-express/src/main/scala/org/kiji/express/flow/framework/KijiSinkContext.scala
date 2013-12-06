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

package org.kiji.express.flow.framework

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.{EntityIdFactory, KijiBufferedWriter}
import org.kiji.express.flow.ColumnOutputSpec

/**
 * Container for the table writer and Kiji table layout required during a sink
 * operation to write the output of a map reduce task back to a Kiji table.
 * This is configured during the sink prepare operation.
 *
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[express] case class KijiSinkContext (
    columns: Map[Symbol, ColumnOutputSpec],
    eidFactory: EntityIdFactory,
    writer: KijiBufferedWriter)
