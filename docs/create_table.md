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
The defined fields need not be included in the data. Fields not included in the data are NULL.

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
|DATE|**false**|
|INTERVAL|**false**|

### String Types
| Type       | Supported    |
|:-----------|:------------:|
|STRING|**true**|
|VARCHAR|**false**|
|CHAR|**false**|

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
If the defined type is different from the data type, cast as much as possible.
If it can not be done, it is NULL.

## Compression class
Please see the list of supported compression formats.

## Make complicated data correspond to vectorization
Hive does not work on complex schema tables in vectorization and pushdown in some cases.
Data handled by Hive can be processed efficiently by using a flat schema, but it is not always possible to define a flat schema for data design.

In Yosegi, it is possible to process complicated schemas as a flat schema by changing array expansion and node configuration.
