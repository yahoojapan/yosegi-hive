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

package jp.co.yahoo.yosegi.message.parser.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

public class HivePrimitiveConverterFactory {

  /**
   * Convert ObjectInspector to corresponding IHivePrimitiveConverter.
   */
  public static IHivePrimitiveConverter get( final ObjectInspector objectInspector ) {

    switch ( objectInspector.getCategory() ) {
      case PRIMITIVE:
        PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector)objectInspector;
        switch ( primitiveInspector.getPrimitiveCategory() ) {
          case BINARY:
            return new HiveBytesPrimitiveConverter( (BinaryObjectInspector)objectInspector );
          case BOOLEAN:
            return new HiveBooleanPrimitiveConverter( (BooleanObjectInspector)objectInspector );
          case BYTE:
            return new HiveBytePrimitiveConverter( (ByteObjectInspector)objectInspector );
          case CHAR:
            return new HiveCharPrimitiveConverter( (HiveCharObjectInspector)objectInspector );
          case DOUBLE:
            return new HiveDoublePrimitiveConverter( (DoubleObjectInspector)objectInspector );
          case FLOAT:
            return new HiveFloatPrimitiveConverter( (FloatObjectInspector)objectInspector );
          case INT:
            return new HiveIntegerPrimitiveConverter( (IntObjectInspector)objectInspector );
          case LONG:
            return new HiveLongPrimitiveConverter( (LongObjectInspector)objectInspector );
          case SHORT:
            return new HiveShortPrimitiveConverter( (ShortObjectInspector)objectInspector );
          case STRING:
            return new HiveStringPrimitiveConverter( (StringObjectInspector)objectInspector );
          case TIMESTAMP:
            return new HiveTimestampPrimitiveConverter( (TimestampObjectInspector)objectInspector );
          case DATE:
            return new HiveDatePrimitiveConverter( (DateObjectInspector)objectInspector );
          case VARCHAR:
            return new HiveVarcharPrimitiveConverter( (HiveVarcharObjectInspector)objectInspector );
          case VOID:
          case UNKNOWN:
          default:
            return new HiveDefaultPrimitiveConverter();
        }
      default :
        return new HiveDefaultPrimitiveConverter();
    }
  }

}
