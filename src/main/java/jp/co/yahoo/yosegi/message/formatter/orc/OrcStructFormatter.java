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
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OrcStructFormatter implements IOrcFormatter {

  private final List<Object> container;
  private final List<KeyAndFormatter> childContainer;

  /**
   * Create new instance.
   */
  public OrcStructFormatter( final StructTypeInfo typeInfo ) {
    container = new ArrayList<Object>();
    childContainer = new ArrayList<KeyAndFormatter>();

    for ( String fieldName : typeInfo.getAllStructFieldNames() ) {
      TypeInfo childTypeInfo = typeInfo.getStructFieldTypeInfo( fieldName );
      childContainer.add( new KeyAndFormatter( fieldName , childTypeInfo ) );
    }
  }

  @Override
  public Object write( final Object obj ) throws IOException {
    container.clear();
    if ( obj instanceof Map ) {
      writeMap( (Map<Object,Object>)obj );
    } else if ( obj instanceof List ) {
      writeList( (List<Object>)obj );
    }
    return container;
  }

  private void writeMap( final Map<Object,Object> obj ) throws IOException {
    for ( KeyAndFormatter childFormatter : childContainer ) {
      childFormatter.clear();
      container.add( childFormatter.get( obj ) );
    }
  }

  private void writeList( final List<Object> obj ) throws IOException {
    for ( int i = 0 ; i < childContainer.size() ; i++ ) {
      KeyAndFormatter childFormatter = childContainer.get(i);
      childFormatter.clear();
      Object inObj = null;
      if ( i < obj.size() ) {
        inObj = obj.get(i);
      }
      container.add( childFormatter.get( inObj ) );
    }
  }

  @Override
  public Object writeFromParser(
      final PrimitiveObject obj , final IParser parser ) throws IOException {
    List<Object> retVal = new ArrayList<Object>();
    for ( KeyAndFormatter keyAndFormatter : childContainer ) {
      retVal.add( keyAndFormatter.writeFromParser( parser ) );
    }
    return retVal;
  }

  @Override
  public void clear() throws IOException {
    container.clear();
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
