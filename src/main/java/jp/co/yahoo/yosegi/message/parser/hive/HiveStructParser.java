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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveStructParser implements IHiveParser {

  private final StructObjectInspector inspector;
  private final List<? extends StructField> fieldList;
  private final boolean[] hasParser;
  private final IHivePrimitiveConverter[] converters;

  private Map<String,Integer> fieldIndexMap;
  private Object row;

  /**
   * Initialize struct information set.
   */
  public HiveStructParser( final StructObjectInspector structObjectInspector ) {
    this.inspector = structObjectInspector;
    fieldIndexMap = new HashMap<String,Integer>();

    fieldList = structObjectInspector.getAllStructFieldRefs();
    hasParser = new boolean[ fieldList.size() ];
    converters = new IHivePrimitiveConverter[ fieldList.size() ];
    for ( int i = 0 ; i < fieldList.size() ; i++ ) {
      StructField field = fieldList.get(i);
      fieldIndexMap.put( field.getFieldName() , Integer.valueOf( i ) );
      hasParser[i] = HiveParserFactory.hasParser( fieldList.get( i ).getFieldObjectInspector() );
      converters[i] =
          HivePrimitiveConverterFactory.get( fieldList.get( i ).getFieldObjectInspector() );
    }
  }

  public void setFieldIndexMap( final Map<String,Integer> fieldIndexMap ) {
    this.fieldIndexMap = fieldIndexMap;
  }

  @Override
  public void setObject( final Object row ) throws IOException {
    this.row = row;
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException {
    if ( ! containsKey( key ) ) {
      return null;
    }
    return get( fieldIndexMap.get( key ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException {
    return converters[index].get( inspector.getStructFieldData( row, fieldList.get( index ) ) );
  }

  @Override
  public IParser getParser( final String key ) throws IOException {
    if ( ! containsKey( key ) ) {
      return new HiveNullParser();
    }
    return getParser( fieldIndexMap.get( key ) );
  }

  @Override
  public IParser getParser( final int index ) throws IOException {
    Object childRow = inspector.getStructFieldData( row, fieldList.get( index ) );
    if ( childRow == null ) {
      return new HiveNullParser();
    }
    IHiveParser childParser =
        HiveParserFactory.get( fieldList.get( index ).getFieldObjectInspector() );
    childParser.setObject( childRow );
    return childParser;
  }

  @Override
  public String[] getAllKey() throws IOException {
    String[] keys = new String[fieldIndexMap.size()];
    int index = 0;
    for ( Map.Entry<String,Integer> entry :  fieldIndexMap.entrySet() ) {
      keys[index] = entry.getKey();
      index++;
    }

    return keys;
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    return fieldIndexMap.containsKey( key );
  }

  @Override
  public int size() throws IOException {
    return fieldList.size();
  }

  @Override
  public boolean isArray() throws IOException {
    return false;
  }

  @Override
  public boolean isMap() throws IOException {
    return false;
  }

  @Override
  public boolean isStruct() throws IOException {
    return true;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException {
    return hasParser[index];
  }

  @Override
  public boolean hasParser( final String key ) throws IOException {
    if ( ! containsKey( key ) ) {
      return false;
    }
    return hasParser( fieldIndexMap.get( key ) );
  }

  @Override
  public Object toJavaObject() throws IOException {
    Map<String,Object> result = new HashMap<String,Object>();
    for ( int i = 0 ; i < size() ; i++ ) {
      String key = fieldList.get(i).getFieldName();
      if ( hasParser(key) ) {
        result.put( key , getParser(key).toJavaObject() );
      } else {
        result.put( key , get(key) );
      }
    }

    return result;
  }

}
