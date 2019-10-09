/**
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
package org.apache.flink.streaming.connectors.pulsar

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.pulsar.internal.{Deserializer, JSONOptionsInRead, PulsarMetadataReader, PulsarRowDeserializer, Utils}
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter
import org.apache.flink.types.Row

import org.apache.pulsar.common.schema.SchemaInfo

class FlinkPulsarSource(parameters: Properties) extends GenericFlinkPulsarSource[Row](parameters) {
  override protected def buildDeserializer(pulsarSchema: SchemaInfo,
                                           jsonOpts: JSONOptionsInRead): Deserializer[Row] = {
    new PulsarRowDeserializer(pulsarSchema, jsonOpts)
  }

  @transient @volatile lazy val inferredSchema = Utils.tryWithResource(
    PulsarMetadataReader(adminUrl, clientConf, "", caseInsensitiveParams)) { reader =>
    val topics = reader.getTopics()
    reader.getSchema(topics)
  }

  override def getProducedType: TypeInformation[Row] = {
    LegacyTypeInfoDataTypeConverter
      .toLegacyTypeInfo(inferredSchema)
      .asInstanceOf[TypeInformation[Row]]
  }

}

