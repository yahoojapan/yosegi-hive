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
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.util.ArrayList;

public class HiveStructSchema implements IHiveSchema {

  private final TypeInfo hiveSchema;
  private final IField schema;

  /**
   * Initialize struct information set.
   */
  public HiveStructSchema( final StructContainerField schema ) throws IOException {
    this.schema = schema;

    StructTypeInfo structSchema = new StructTypeInfo();
    ArrayList<String> childKey = new ArrayList<String>();
    ArrayList<TypeInfo> childTypeInfo = new ArrayList<TypeInfo>();
    for ( String key : schema.getKeys() ) {
      TypeInfo typeInfo = HiveSchemaFactory.getHiveSchema( schema.get( key ) );
      childKey.add( key );
      childTypeInfo.add( typeInfo );
    }
    structSchema.setAllStructFieldNames( childKey );
    structSchema.setAllStructFieldTypeInfos( childTypeInfo );

    hiveSchema = structSchema;
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
