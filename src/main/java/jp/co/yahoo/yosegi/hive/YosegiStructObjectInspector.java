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

import jp.co.yahoo.yosegi.hive.io.ColumnAndIndex;
import jp.co.yahoo.yosegi.hive.io.PrimitiveToWritableConverter;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YosegiStructObjectInspector extends SettableStructObjectInspector {

  private final List<StructField> fields;
  private final Map<String,StructField> fieldsMap;

  private interface YosegiStructField extends StructField {

    Object getValue( final ColumnAndIndex columnAndIndex );

  }

  private static class YosegiPrimitiveStructField implements YosegiStructField {
    private final int fieldId;
    private final String fieldName;
    private final PrimitiveObjectInspector inspector;
    private final PrimitiveCategory category;
    private int currentColumnIndex = -1;
    private IColumn childColumn;

    public YosegiPrimitiveStructField(
        final int fieldId , final String fieldName , final ObjectInspector inspector ) {
      this.fieldId = fieldId;
      this.fieldName = fieldName;
      this.inspector = (PrimitiveObjectInspector)inspector;
      this.category = this.inspector.getPrimitiveCategory();
    }

    @Override
    public Object getValue( final ColumnAndIndex columnAndIndex ) {
      if ( currentColumnIndex != columnAndIndex.columnIndex ) {
        childColumn = columnAndIndex.column.getColumn( getFieldName() ); 
        currentColumnIndex = columnAndIndex.columnIndex;
      }
      try {
        return PrimitiveToWritableConverter.convert(
            category , childColumn.get( columnAndIndex.index ) );
      } catch ( IOException ex ) {
        throw new UncheckedIOException( ex );
      }
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public int getFieldID() {
      return fieldId;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
  }

  private static class YosegiNestedStructField implements YosegiStructField {

    private final int fieldId;
    private final String fieldName;
    private final ObjectInspector inspector;
    private final ColumnAndIndex childColumnAndIndex;
    private int currentColumnIndex = -1;

    public YosegiNestedStructField(
        final int fieldId , final String fieldName , final ObjectInspector inspector ) {
      this.fieldId = fieldId;
      this.fieldName = fieldName;
      this.inspector = inspector;
      childColumnAndIndex = new ColumnAndIndex();
    }

    @Override
    public Object getValue( final ColumnAndIndex columnAndIndex ) {
      if ( currentColumnIndex != columnAndIndex.columnIndex ) {
        IColumn childColumn = columnAndIndex.column.getColumn( getFieldName() );
        childColumnAndIndex.column = childColumn;
        currentColumnIndex = columnAndIndex.columnIndex;
      }
      return new ColumnAndIndex(
          childColumnAndIndex.column ,
          columnAndIndex.index ,
          columnAndIndex.columnIndex );
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public int getFieldID() {
      return fieldId;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
  }

  private static class YosegiUnionStructField implements YosegiStructField {

    private final int fieldId;
    private final String fieldName;
    private final StandardUnionObjectInspector inspector;
    private final ColumnAndIndex childColumnAndIndex;
    private final UnionField unionField;

    public YosegiUnionStructField(
        final int fieldId ,
        final String fieldName ,
        final StandardUnionObjectInspector inspector ,
        final UnionTypeInfo unionTypeInfo ) {
      this.fieldId = fieldId;
      this.fieldName = fieldName;
      this.inspector = inspector;
      childColumnAndIndex = new ColumnAndIndex();
      childColumnAndIndex.columnIndex = -1;
      unionField = new UnionField( unionTypeInfo );
    }

    @Override
    public Object getValue( final ColumnAndIndex columnAndIndex ) {
      if ( childColumnAndIndex.columnIndex != columnAndIndex.columnIndex ) {
        IColumn childColumn = columnAndIndex.column.getColumn( getFieldName() );
        childColumnAndIndex.column = childColumn;
        childColumnAndIndex.columnIndex = columnAndIndex.columnIndex;
      }
      childColumnAndIndex.index = columnAndIndex.index;
      
      return unionField.get( childColumnAndIndex );
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public int getFieldID() {
      return fieldId;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
  }

  /**
   * Initialize by setting struct type information.
   */
  public YosegiStructObjectInspector( final StructTypeInfo typeInfo ) {
    fieldsMap = new HashMap<String,StructField>();

    List<String> fieldNameList = typeInfo.getAllStructFieldNames();
    List<TypeInfo> fieldTypeList = typeInfo.getAllStructFieldTypeInfos();

    fields = new ArrayList<StructField>( fieldNameList.size() );
    for ( int i = 0 ; i < fieldNameList.size(); i++ ) {
      if ( fieldTypeList.get(i).getCategory() ==  Category.PRIMITIVE ) {
        StructField field = new YosegiPrimitiveStructField(
            i ,
            fieldNameList.get(i) ,
            YosegiObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( fieldTypeList.get(i) )
            );
        fields.add( field );
        fieldsMap.put( fieldNameList.get(i) , field );
      } else if ( fieldTypeList.get(i).getCategory() ==  Category.UNION ) {
        StructField field = new YosegiUnionStructField(
            i ,
            fieldNameList.get(i) ,
            (StandardUnionObjectInspector)(
              YosegiObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( fieldTypeList.get(i) )
            ) ,
            (UnionTypeInfo)( fieldTypeList.get(i) ) );
        fields.add( field );
        fieldsMap.put( fieldNameList.get(i) , field );
      } else {
        StructField field = new YosegiNestedStructField(
            i ,
            fieldNameList.get(i) ,
            YosegiObjectInspectorFactory
              .craeteObjectInspectorFromTypeInfo( fieldTypeList.get(i) ) );
        fields.add( field );
        fieldsMap.put( fieldNameList.get(i) , field );
      }
    }
  }

  @Override
  public List<StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public StructField getStructFieldRef( final String str ) {
    return fieldsMap.get( str );
  }

  @Override
  public Object getStructFieldData( final Object object, final StructField field ) {
    if ( object instanceof ColumnAndIndex ) {
      YosegiStructField structFiled = (YosegiStructField)field;
      return structFiled.getValue( (ColumnAndIndex) object );
    } else {
      return ( (List<Object>)object ).get( field.getFieldID() );
    }
  }

  @Override
  public List<Object> getStructFieldsDataAsList( final Object object ) {
    if ( object instanceof List ) {
      return (List<Object>) object;
    }

    ColumnAndIndex columnAndIndex = (ColumnAndIndex) object;
    List<Object> result = new ArrayList<Object>( fields.size() );
    for ( StructField field : fields ) {
      YosegiStructField structFiled = (YosegiStructField)field;
      result.add( structFiled.getValue( (ColumnAndIndex)object ) );
    }
    return result;
  }

  @Override
  public String getTypeName() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("struct<");
    for ( int i = 0 ; i < fields.size() ; ++i ) {
      StructField field = fields.get(i);
      if (i != 0) {
        buffer.append(",");
      }
      buffer.append( field.getFieldName() );
      buffer.append(":");
      buffer.append(field.getFieldObjectInspector().getTypeName());
    }
    buffer.append(">");
    return buffer.toString();
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public Object create() {
    List<Object> result = new ArrayList<Object>( fields.size() );
    for ( int i = 0 ; i < fields.size() ; i++ ) {
      result.add( null );
    }
    return result;
  }

  @Override
  public Object setStructFieldData(
      final Object object, final StructField field , final Object fieldValue ) {
    ( (List<Object>)object ).set( field.getFieldID() , fieldValue );

    return object;
  }

  @Override
  public boolean equals( final Object obj ) {
    if ( obj == null || obj.getClass() != getClass() ) {
      return false;
    } else if ( obj == this ) {
      return true;
    } else {
      List<StructField> other = ( (YosegiStructObjectInspector) obj ).fields;
      if ( other.size() != fields.size() ) {
        return false;
      }
      for ( int i = 0; i < fields.size(); ++i ) {
        StructField left = other.get(i);
        StructField right = fields.get(i);
        if ( ! ( left.getFieldName().equalsIgnoreCase(right.getFieldName() )
            && left.getFieldObjectInspector().equals( right.getFieldObjectInspector() )
          ) ) {
          return false;
        }
      }
      return true;
    }
  }

}
