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

package jp.co.yahoo.yosegi.message.formatter.orc;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

public class OrcFormatterFactory {

  /**
   * Get IOrcFormatter from TypeInfo.
   */
  public static IOrcFormatter get( final TypeInfo typeInfo ) {
    if ( typeInfo.getCategory()  == ObjectInspector.Category.LIST ) {
      return new OrcListFormatter( (ListTypeInfo)typeInfo );
    } else if ( typeInfo.getCategory()  == ObjectInspector.Category.MAP ) {
      return new OrcMapFormatter( (MapTypeInfo)typeInfo );
    } else if ( typeInfo.getCategory()  == ObjectInspector.Category.STRUCT ) {
      return new OrcStructFormatter( (StructTypeInfo)typeInfo );
    } else if ( typeInfo.getCategory()  == ObjectInspector.Category.UNION ) {
      return new OrcUnionFormatter( (UnionTypeInfo)typeInfo );
    } else if ( typeInfo.getCategory()  == ObjectInspector.Category.PRIMITIVE ) {
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo)typeInfo;
      if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.BINARY ) {
        return new OrcBytesFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN ) {
        return new OrcBooleanFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.BYTE ) {
        return new OrcByteFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE ) {
        return new OrcDoubleFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.FLOAT ) {
        return new OrcFloatFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.INT ) {
        return new OrcIntegerFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.LONG ) {
        return new OrcLongFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.SHORT ) {
        return new OrcShortFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.STRING ) {
        return new OrcStringFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP ) {
        return new OrcTimestampFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.VOID ) {
        return new OrcVoidFormatter();
      } else if ( primitiveTypeInfo.getPrimitiveCategory()
          == PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN ) {
        return new OrcVoidFormatter();
      } else {
        return new OrcNullFormatter();
      }
    } else {
      return new OrcNullFormatter();
    }
  }

}
