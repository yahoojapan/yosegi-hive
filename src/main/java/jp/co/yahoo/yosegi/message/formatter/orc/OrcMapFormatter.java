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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OrcMapFormatter implements IOrcFormatter {

  private final Map<Object,Object> container;
  private final MapTypeInfo typeInfo;

  public OrcMapFormatter( final MapTypeInfo typeInfo ) {
    this.typeInfo = typeInfo;
    container = new HashMap<Object,Object>();
  }

  @Override
  public Object write( final Object obj ) throws IOException {
    container.clear();
    if ( ! ( obj instanceof Map ) ) {
      return container;
    }

    Map<Object,Object> mapObj = (Map<Object,Object>)obj;
    for ( Map.Entry<Object,Object> entry : mapObj.entrySet() ) {
      IOrcFormatter childFormatter = OrcFormatterFactory.get( typeInfo.getMapValueTypeInfo() );
      container.put( entry.getKey() , childFormatter.write( entry.getValue() ) );
    }

    return container;
  }

  @Override
  public Object writeFromParser(
      final PrimitiveObject obj , final IParser parser ) throws IOException {
    container.clear();
    String[] keys = parser.getAllKey();
    for ( int i = 0 ; i < keys.length ; i++ ) {
      IOrcFormatter childFormatter =
          OrcFormatterFactory.get( typeInfo.getMapValueTypeInfo() );
      container.put( keys[i] , childFormatter.writeFromParser(
          parser.get( keys[i] ) , parser.getParser( keys[i] ) ) );
    }

    return container;
  }

  @Override
  public void clear() throws IOException {
    container.clear();
  }

}
