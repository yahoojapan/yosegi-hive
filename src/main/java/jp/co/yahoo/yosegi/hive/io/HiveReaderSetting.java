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

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.hive.pushdown.HiveExprOrNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HiveReaderSetting implements IReaderSetting {

  private final Configuration config;
  private final IExpressionNode node;
  private final boolean isVectorModeFlag;
  private final boolean disableSkipBlock;
  private final boolean disableFilterPushdown;

  /**
   * Initialize.
   */
  public HiveReaderSetting(
      final Configuration config ,
      final IExpressionNode node ,
      final boolean isVectorModeFlag ,
      final boolean disableSkipBlock ,
      final boolean disableFilterPushdown ) {
    this.config = config;
    this.node = node;
    this.isVectorModeFlag = isVectorModeFlag;
    this.disableSkipBlock = disableSkipBlock;
    this.disableFilterPushdown = disableFilterPushdown;
  }

  /**
   * |Set the object to be read and initialize.
   */
  public HiveReaderSetting( final FileSplit split, final JobConf job ) {
    config = new Configuration();

    disableSkipBlock = job.getBoolean( "yosegi.disable.block.skip" , false );
    disableFilterPushdown = job.getBoolean( "yosegi.disable.filter.pushdown" , true );

    List<ExprNodeGenericFuncDesc> filterExprs = new ArrayList<ExprNodeGenericFuncDesc>();
    String filterExprSerialized = job.get( TableScanDesc.FILTER_EXPR_CONF_STR );
    if ( filterExprSerialized != null ) {
      filterExprs.add( SerializationUtilities.deserializeExpression( filterExprSerialized ) );
    }

    MapWork mapWork;
    try {
      mapWork = Utilities.getMapWork(job);
    } catch ( Exception ex ) {
      mapWork = null;
    }

    if ( mapWork == null ) {
      if (job.get("spread.reader.expand.column") != null) {
        config.set("spread.reader.expand.column", job.get("spread.reader.expand.column"));
      }
      Iterator<Map.Entry<String,String>> jobConfIterator = job.iterator();
      while ( jobConfIterator.hasNext() ) {
        Map.Entry<String,String> keyValue = jobConfIterator.next();
        if ( keyValue.getKey().startsWith( "spread.reader.flatten.column" ) ) {
          config.set( keyValue.getKey() , keyValue.getValue() );
        }
      }
      node = createExpressionNode( filterExprs );
      isVectorModeFlag = false;
      return;
    } else {
      for ( Map.Entry<Path,PartitionDesc> pathsAndParts
          : mapWork.getPathToPartitionInfo().entrySet() ) {
        Properties props = pathsAndParts.getValue().getTableDesc().getProperties();
        if ( props.containsKey( "yosegi.expand" ) ) {
          config.set( "spread.reader.expand.column" , props.getProperty( "yosegi.expand" ) );
        }
        Iterator<String> iterator = props.stringPropertyNames().iterator();
        while ( iterator.hasNext() ) {
          String keyName = iterator.next();
          if ( keyName.startsWith( "yosegi.flatten" ) ) {
            String yosegiKeyName = keyName.replace(
                "yosegi.flatten" , "spread.reader.flatten.column" );
            config.set( yosegiKeyName , props.getProperty( keyName ) );
          }
        }
      }
    }

    node = createExpressionNode( filterExprs );

    config.set( "spread.reader.read.column.names" , createReadColumnNames(
        job.get( ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR , null ) ) );

    isVectorModeFlag = Utilities.getIsVectorized( job );
  }

  /**
   * Create the setting of the column to be read by Yosegi.
   */
  public String createReadColumnNames( final String readColumnNames ) {
    if ( readColumnNames == null || readColumnNames.isEmpty() ) {
      return null;
    }
    StringBuilder jsonStringBuilder = new StringBuilder();
    jsonStringBuilder.append( "[" );
    int addCount = 0;
    for ( String readColumnName : readColumnNames.split( "," ) ) {
      if ( readColumnName.isEmpty() ) {
        continue;
      }
      if ( addCount != 0 ) {
        jsonStringBuilder.append( "," );
      }
      jsonStringBuilder.append( "[\"" );
      jsonStringBuilder.append( readColumnName );
      jsonStringBuilder.append( "\"]" );
      addCount++;
    }
    jsonStringBuilder.append( "]" );
    return jsonStringBuilder.toString();
  }

  /**
   * Convert Hive filter condition to Yosegi filter condition.
   */
  public IExpressionNode createExpressionNode( final List<ExprNodeGenericFuncDesc> filterExprs ) {
    HiveExprOrNode hiveOrNode = new HiveExprOrNode();
    for ( ExprNodeGenericFuncDesc filterExpr : filterExprs ) {
      if ( filterExpr != null ) {
        hiveOrNode.addChildNode( filterExpr );
      }
    }

    return hiveOrNode.getPushDownFilterNode();
  }

  /**
   * Create a Set containing path candidates.
   */
  public Set<String> createPathSet( final Path target ) {
    Set<String> result = new HashSet<String>();
    result.add( target.toString() );
    result.add( target.toUri().toString() );
    result.add( target.getParent().toUri().toString() );

    return result;
  }

  @Override
  public boolean isVectorMode() {
    return isVectorModeFlag;
  }

  @Override
  public boolean isDisableSkipBlock() {
    return disableSkipBlock;
  }

  @Override
  public boolean isDisableFilterPushdown() {
    return disableFilterPushdown;
  }

  @Override
  public Configuration getReaderConfig() {
    return config;
  }

  @Override
  public IExpressionNode getExpressionNode() {
    return node;
  }

}
