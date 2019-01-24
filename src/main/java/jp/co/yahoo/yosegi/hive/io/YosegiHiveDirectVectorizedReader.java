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

package jp.co.yahoo.yosegi.hive.io;

import jp.co.yahoo.yosegi.hive.io.vector.IColumnVectorAssignor;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.IndexFactory;
import jp.co.yahoo.yosegi.stats.SummaryStats;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class YosegiHiveDirectVectorizedReader
    implements RecordReader<NullWritable, VectorizedRowBatch> {

  private static final Logger LOG =
      LoggerFactory.getLogger( YosegiHiveDirectVectorizedReader.class );

  private final YosegiReader currentReader = new YosegiReader();
  private final IJobReporter reporter;
  private final IColumnVectorAssignor[] assignors;
  private final IExpressionNode node;
  private final IVectorizedReaderSetting setting;
  private final int[] needColumnIds;
  private final String[] columnNames;

  private boolean isEnd;
  private IExpressionIndex currentIndexList;
  private int currentIndex;
  private int indexSize;
  private int readSpreadCount;

  /**
   * InputStream is set and initialized.
   */
  public YosegiHiveDirectVectorizedReader(
      final InputStream in ,
      final long dataLength ,
      final long start ,
      final long length ,
      final IVectorizedReaderSetting setting ,
      final IJobReporter reporter ) throws IOException {
    this.setting = setting;
    this.reporter = reporter;
    node = setting.getExpressionNode();
    if ( ! setting.isDisableSkipBlock() ) {
      currentReader.setBlockSkipIndex( node );
    }
    currentReader.setNewStream( in ,  dataLength , setting.getReaderConfig(), start , length );
    assignors = setting.getAssignors();
    needColumnIds = setting.getNeedColumnIds();
    columnNames = setting.getColumnNames();
  }

  @Override
  public void close() throws IOException {
    currentReader.close();
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    return setting.createVectorizedRowBatch();
  }

  @Override
  public long getPos() throws IOException {
    return currentReader.getReadPos();
  }

  @Override
  public float getProgress() throws IOException {
    return (float)currentReader.getBlockReadCount() / (float)currentReader.getBlockCount();
  }

  private boolean setSpread() throws IOException {
    if ( isEnd ) {
      return false;
    }
    Spread spread = currentReader.next();
    readSpreadCount++;
    if ( setting.isDisableFilterPushdown() ) {
      currentIndexList = IndexFactory.toExpressionIndex( spread , null );
    } else {
      currentIndexList = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
    }
  
    indexSize = currentIndexList.size();
    currentIndex = 0;
    if ( indexSize == 0 ) {
      return false;
    }
    for ( int colIndex : needColumnIds ) {
      String columnName = columnNames[colIndex];
      assignors[colIndex].setColumn( indexSize , spread.getColumn( columnName ) );
    }
    return true;
  }

  private void updateCounter( final SummaryStats stats ) {
    if ( ! isEnd ) {
      reporter.incrCounter( "Yosegi_STATS" , "ROWS" , stats.getRowCount() );
      reporter.incrCounter( "Yosegi_STATS" , "RAW_DATA_SIZE" , stats.getRawDataSize() );
      reporter.incrCounter( "Yosegi_STATS" , "REAL_DATA_SIZE" , stats.getRealDataSize() );
      reporter.incrCounter( "Yosegi_STATS" , "LOGICAL_DATA_SIZE" , stats.getLogicalDataSize() );
      reporter.incrCounter( "Yosegi_STATS" , "LOGICAL_TOTAL_CARDINALITY" , stats.getCardinality() );
      reporter.incrCounter( "Yosegi_STATS" , "SPREAD" , readSpreadCount );
    }
  }

  @Override
  public boolean next(
      final NullWritable key, final VectorizedRowBatch outputBatch ) throws IOException {
    outputBatch.reset();
    setting.setPartitionValues( outputBatch );

    if ( indexSize <= currentIndex ) {
      if ( ! currentReader.hasNext() ) {
        updateCounter( currentReader.getReadStats() );
        outputBatch.endOfFile = true;
        isEnd = true;
        return false;
      }
      while ( ! setSpread() ) {
        if ( ! currentReader.hasNext() ) {
          updateCounter( currentReader.getReadStats() );
          outputBatch.endOfFile = true;
          isEnd = true;
          return false;
        }
      }
    }
    int maxSize = outputBatch.getMaxSize();
    if ( indexSize < currentIndex + maxSize ) {
      maxSize = indexSize - currentIndex;
    }

    for ( int colIndex : needColumnIds ) {
      assignors[colIndex].setColumnVector(
          outputBatch.cols[colIndex] , currentIndexList , currentIndex , maxSize );
    }
    outputBatch.size = maxSize;

    currentIndex += maxSize;
    if ( indexSize <= currentIndex && ! currentReader.hasNext() ) {
      outputBatch.endOfFile = true;
    }

    return outputBatch.size > 0;
  }

}
