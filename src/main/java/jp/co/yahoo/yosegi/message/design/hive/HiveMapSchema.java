/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.message.design.hive;

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;

import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.io.IOException;

public class HiveMapSchema implements IHiveSchema {

  private final TypeInfo hiveSchema;
  private final IField schema;

  /**
   * Initialize map information set.
   */
  public HiveMapSchema( final MapContainerField schema ) throws IOException {
    this.schema = schema;

    MapTypeInfo mapSchema = new MapTypeInfo();
    mapSchema.setMapKeyTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfo( TypeInfoFactory.stringTypeInfo.getTypeName() ) );
    mapSchema.setMapValueTypeInfo( HiveSchemaFactory.getHiveSchema( schema.getField() ) );
    hiveSchema = mapSchema;
  }

  @Override
  public IField getGeneralSchema() throws IOException {
    return schema;
  }

  @Override
  public TypeInfo getHiveSchema() throws IOException {
    return hiveSchema;
  }

}
