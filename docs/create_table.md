<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# About creating tables

See the [quick start](quickstart.md) for the procedure up to creating the table.

As with JSON, Yosegi maps fields and columns by name.
Yosegi treats the fields that have not been appeared in the data as NULL when user access them.

## Configuration and TBLPROPERTIES

### Configuration

| variable | summary |
|:-----------|:------------|
| yosegi.disable.block.skip | Disable Pushdown. |
| yosegi.disable.filter.pushdown | Disable Pushdown. |

### TBLPROPERTIES

| variable | summary |
|:-----------|:------------|
| yosegi.expand | Use expand function. |
| yosegi.flatten | Use flatten function. |
| yosegi.spread.size | The maximum size that can be written to Spread. |
| yosegi.record.writer.max.rows | Maximum number of lines that can be written to Spread. |
| yosegi.compression.class | Default compression class. |
| yosegi.compress.optimize.allowed.ratio | Ratio to allow optimization on compression. |

## Support data type

### Numeric Types
| Type       | Supported    |
|:-----------|:------------:|
|TINYINT|**true**|
|SMALLINT|**true**|
|INTEGER|**true**|
|FLOAT|**true**|
|DOUBLE|**true**|
|DECIMAL|**false**|

### Date/Time Types
| Type       | Supported    |
|:-----------|:------------:|
|TIMESTAMP|**true**|
|DATE|**true**|
|INTERVAL|**false**|

### String Types
| Type       | Supported    |
|:-----------|:------------:|
|STRING|**true**|
|VARCHAR|**true**|
|CHAR|**true**|

### Misc Types
| Type       | Supported    |
|:-----------|:------------:|
|BOOLEAN|**true**|
|BINARY|**true**|

### Complex Types
| Type       | Supported    |
|:-----------|:------------:|
|ARRAYS|**true**|
|MAPS|**true**|
|STRUCT|**true**|
|UNION|**true**|

## Cast of type
Yosegi tries to cast data type to request one that is different from the estimated one based on input data. If Yosegi fails to change the data type, it treats the data as NULL.

## Compression class
Please see the list of supported compression formats.

## Make complicated data correspond to vectorization
Hive cannot work on complex schema tables in vectorization and pushdown in some cases.
Hive can handle efficiently the data that is defined by a flat schema. However, it is not always possible to use a flat schema for data design.

On the other hand, Yosegi can change complex schemas as a flat schema using array expansion and node configuration.
