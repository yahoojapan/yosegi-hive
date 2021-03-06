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

import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OrcListFormatter implements IOrcFormatter {

  private final List<Object> container;
  private final ListTypeInfo typeInfo;

  public OrcListFormatter( final ListTypeInfo typeInfo ) {
    this.typeInfo = typeInfo;
    container = new ArrayList<Object>();
  }

  @Override
  public Object write( final Object obj ) throws IOException {
    container.clear();
    if ( ! ( obj instanceof List ) ) {
      return container;
    }

    List<Object> listObj = (List)obj;
    for ( Object childObj : listObj ) {
      IOrcFormatter childFormatter =
          OrcFormatterFactory.get( typeInfo.getListElementTypeInfo() );
      container.add( childFormatter.write( childObj ) );
    }

    return container;
  }

  @Override
  public Object writeFromParser(
      final PrimitiveObject obj , final IParser parser ) throws IOException {
    container.clear();
    for ( int i = 0 ; i < parser.size() ; i++ ) {
      IOrcFormatter childFormatter =
          OrcFormatterFactory.get( typeInfo.getListElementTypeInfo() );
      container.add( childFormatter.writeFromParser( parser.get(i) , parser.getParser(i) ) );
    }
    return container;
  }

  @Override
  public void clear() throws IOException {
    container.clear();
  }

}
