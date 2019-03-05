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

package jp.co.yahoo.yosegi.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.util.ArrayList;
import java.util.List;

public final class YosegiObjectInspectorFactory {

  private YosegiObjectInspectorFactory() {}

  /**
   * Create ObjectInspector from TypeInfo.
   */
  public static ObjectInspector craeteObjectInspectorFromTypeInfo( final TypeInfo typeInfo ) {
    switch ( typeInfo.getCategory() ) {
      case STRUCT:
        return new YosegiStructObjectInspector( (StructTypeInfo)typeInfo );
      case MAP:
        return new YosegiMapObjectInspector( (MapTypeInfo)typeInfo );
      case LIST:
        return new YosegiListObjectInspector( (ListTypeInfo)typeInfo );
      case UNION:
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo)typeInfo;
        List<ObjectInspector> unionList = new ArrayList<ObjectInspector>();
        for ( TypeInfo childTypeInfo : unionTypeInfo.getAllUnionObjectTypeInfos() ) {
          unionList.add( craeteObjectInspectorFromTypeInfo( childTypeInfo ) );
        }
        return ObjectInspectorFactory.getStandardUnionObjectInspector( unionList );
      case PRIMITIVE:
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo)typeInfo;
        switch ( primitiveTypeInfo.getPrimitiveCategory() ) {
          case STRING:
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
          case CHAR:
            return PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector;
          case BINARY:
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
          case BOOLEAN:
            return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
          case BYTE:
            return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
          case DOUBLE:
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
          case FLOAT:
            return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
          case INT:
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
          case LONG:
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
          case SHORT:
            return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
          case TIMESTAMP:
            return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
          case DATE:
            return PrimitiveObjectInspectorFactory.writableDateObjectInspector;

          case DECIMAL:
          case VOID:
          default:
            throw new UnsupportedOperationException(
          "Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory() );
        }
      default:
        throw new UnsupportedOperationException( "Unknown category " + typeInfo.getCategory() );
    }
  }

}
