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

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class HiveParserFactory {

  /**
   * Create IParser from ObjectInspector.
   */
  public static IHiveParser get( final ObjectInspector objectInspector ) {

    switch ( objectInspector.getCategory() ) {
      case LIST:
        return new HiveListParser( (ListObjectInspector)objectInspector );
      case MAP:
        return new HiveMapParser( (MapObjectInspector)objectInspector );
      case STRUCT:
        return new HiveStructParser( (StructObjectInspector)objectInspector );
      case UNION:
      default:
        return new HiveNullParser();
    }
  }

  /**
   * Check whether a child parser exists.
   */
  public static boolean hasParser( final ObjectInspector objectInspector ) {
    switch ( objectInspector.getCategory() ) {
      case LIST:
      case MAP:
      case STRUCT:
        return true;
      case UNION:
      default:
        return false;
    }
  }

}
