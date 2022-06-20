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
package jp.co.yahoo.yosegi.hive.blackbox;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;

import java.io.IOException;

public final class ColumnBinaryTestCase
{
    private ColumnBinaryTestCase() {}

    public static String[] stringClassNames() throws IOException
    {
        return new String[]{
            "jp.co.yahoo.yosegi.binary.maker.DumpBytesColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizeDumpStringColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizeIndexDumpStringColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeStringColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.RleStringColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.DictionaryRleStringColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayStringColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpStringColumnBinaryMaker"};
    }

    public static String[] numberClassNames() throws IOException
    {
        return new String[]{
            "jp.co.yahoo.yosegi.binary.maker.OptimizeDumpLongColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizeLongColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker"};
    }

    public static String[] floatClassNames() throws IOException
    {
        return new String[]{
            "jp.co.yahoo.yosegi.binary.maker.DumpFloatColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizeFloatColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.RangeDumpFloatColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeFloatColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpFloatColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayFloatColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpFloatColumnBinaryMaker"};
    }

    public static String[] doubleClassNames() throws IOException
    {
        return new String[]{
            "jp.co.yahoo.yosegi.binary.maker.DumpDoubleColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizeDoubleColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.RangeDumpDoubleColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDoubleColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpDoubleColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDoubleColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpDoubleColumnBinaryMaker"};
    }

    public static String[] booleanClassNames() throws IOException
    {
        return new String[]{
            "jp.co.yahoo.yosegi.binary.maker.DumpBooleanColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBooleanColumnBinaryMaker",
            "jp.co.yahoo.yosegi.binary.maker.FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker"};
    }

    public static ColumnBinary createColumnBinary(String targetClassName, IColumn column) throws IOException
    {
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
        return maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    }

    public static ColumnBinary createStringColumnBinaryFromString(String[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createStringColumnBinaryFromString(defaultConfig.getColumnMaker(ColumnType.STRING).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createStringColumnBinaryFromString(String targetClassName, String[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.STRING, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.STRING, new StringObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static ColumnBinary createStringColumnBinaryFromBytes(byte[][] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createStringColumnBinaryFromBytes(defaultConfig.getColumnMaker(ColumnType.STRING).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createStringColumnBinaryFromBytes(String targetClassName, byte[][] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.STRING, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.STRING, new BytesObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static ColumnBinary createBooleanColumnBinaryFromBoolean(boolean[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createBooleanColumnBinaryFromBoolean(defaultConfig.getColumnMaker(ColumnType.BOOLEAN).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createBooleanColumnBinaryFromBoolean(String targetClassName, boolean[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.BOOLEAN, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.BOOLEAN, new BooleanObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static ColumnBinary createByteColumnBinaryFromByte(byte[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createByteColumnBinaryFromByte(defaultConfig.getColumnMaker(ColumnType.BYTE).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createByteColumnBinaryFromByte(String targetClassName, byte[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.BYTE, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.BYTE, new ByteObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static ColumnBinary createShortColumnBinaryFromShort(short[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createShortColumnBinaryFromShort(defaultConfig.getColumnMaker(ColumnType.SHORT).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createShortColumnBinaryFromShort(String targetClassName, short[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.SHORT, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.SHORT, new ShortObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static ColumnBinary createIntegerColumnBinaryFromInteger(int[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createIntegerColumnBinaryFromInteger(defaultConfig.getColumnMaker(ColumnType.INTEGER).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createIntegerColumnBinaryFromInteger(String targetClassName, int[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.INTEGER, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.INTEGER, new IntegerObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static ColumnBinary createLongColumnBinaryFromLong(long[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createLongColumnBinaryFromLong(defaultConfig.getColumnMaker(ColumnType.LONG).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createLongColumnBinaryFromLong(String targetClassName, long[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.LONG, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.LONG, new LongObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static ColumnBinary createFloatColumnBinaryFromFloat(float[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createFloatColumnBinaryFromFloat(defaultConfig.getColumnMaker(ColumnType.FLOAT).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createFloatColumnBinaryFromFloat(String targetClassName, float[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.FLOAT, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.FLOAT, new FloatObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static ColumnBinary createDoubleColumnBinaryFromDouble(double[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
        return createDoubleColumnBinaryFromDouble(defaultConfig.getColumnMaker(ColumnType.DOUBLE).getClass().getName(), data, isNullArray, columnName);
    }

    public static ColumnBinary createDoubleColumnBinaryFromDouble(String targetClassName, double[] data, boolean[] isNullArray, String columnName) throws IOException
    {
        IColumn column = new PrimitiveColumn(ColumnType.DOUBLE, columnName);
        for (int i = 0; i < data.length; i++) {
            if (!isNullArray[i]) {
                column.add(ColumnType.DOUBLE, new DoubleObj(data[i]), i);
            }
        }
        return createColumnBinary(targetClassName, column);
    }

    public static IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
        return toColumn(columnBinary, columnBinary.rowCount);
    }

    public static IColumn toColumn(final ColumnBinary columnBinary, Integer loadCount) throws IOException {
        if (loadCount == null) {
            loadCount = (columnBinary.isSetLoadSize) ? columnBinary.loadSize : loadCount;
        }
        return new YosegiLoaderFactory().create(columnBinary, loadCount);
    }
}
