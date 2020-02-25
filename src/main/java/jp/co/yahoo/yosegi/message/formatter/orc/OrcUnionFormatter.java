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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OrcUnionFormatter implements IOrcFormatter {

  private final List<KeyAndFormatter> childContainer;

  /**
   * Create new instance.
   */
  public OrcUnionFormatter( final UnionTypeInfo typeInfo ) {
    childContainer = new ArrayList<KeyAndFormatter>();

    for ( TypeInfo childTypeInfo : typeInfo.getAllUnionObjectTypeInfos() ) {
      childContainer.add(
          new KeyAndFormatter( childTypeInfo.getTypeName() , childTypeInfo ) );
    }
  }

  @Override
  public Object write( final Object obj ) throws IOException {
    if ( obj instanceof PrimitiveObject) {
      return ( (PrimitiveObject)obj ).get();
    }
    return obj;
  }

  @Override
  public Object writeFromParser(
      final PrimitiveObject obj , final IParser parser ) throws IOException {
    return obj.get();
  }

  @Override
  public void clear() throws IOException {
    for ( KeyAndFormatter childFormatter : childContainer ) {
      childFormatter.clear();
    }
  }

  private class KeyAndFormatter {

    private final String key;
    private final IOrcFormatter formatter;

    public KeyAndFormatter( final String key , final TypeInfo typeInfo ) {
      this.key = key;
      formatter = OrcFormatterFactory.get( typeInfo );
    }

    public String getName() {
      return key;
    }

    public Object get( final Object target ) throws IOException {
      return formatter.write( target );
    }

    public Object get( final Map<Object,Object> map ) throws IOException {
      return formatter.write( map.get( key ) );
    }

    public Object writeFromParser( final IParser parser ) throws IOException {
      return formatter.writeFromParser( parser.get( key ) , parser.getParser( key ) );
    }

    public void clear() throws IOException {
      formatter.clear();
    }
  }

}
