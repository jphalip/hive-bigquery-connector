# Hive-BigQuery Connector

The Hive-BigQuery Connector is a Hive StorageHandler that enables Hive to interact with BigQuery's storage layer. It
allows you to keep your existing Hive queries but move data to BigQuery. It utilizes the high throughput
[BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage/) to read and write data.

## Release notes

See the details in [CHANGES.md](CHANGES.md).

## Version support

This connector has been tested with Hive 3.1.2, Hadoop 2.10.1, and Tez 0.9.1.

## Installation

1. Enable the BigQuery Storage API. Follow [these instructions](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api).

2. Clone this repository:
   ```sh
   git clone https://github.com/GoogleCloudPlatform/hive-bigquery-connector
   cd hive-bigquery-connector
   ```

3. Compile and package the JAR:
   ``` sh
   ./mvnw package -DskipTests
   ```
   The packaged JAR is now available at: `connector/target/hive-bigquery-connector-2.0.0-SNAPSHOT-with-dependencies.jar`

4. Copy the packaged JAR to a Google Cloud Storage bucket that can be accessed from your Hive cluster.

5. Open the Hive CLI and load the JAR:

   ```sh
   hive> add jar gs://<JAR location>/hive-bigquery-connector-2.0.0-SNAPSHOT-with-dependencies.jar;
   ```

4. Verify that the JAR is correctly loaded:

   ```sh
   hive> list jars;
   ```

## Managed vs external tables

Hive can have [two types](https://cwiki.apache.org/confluence/display/Hive/Managed+vs.+External+Tables) of tables:

- Managed tables, sometimes referred to as internal tables.
- External tables.

The Hive BigQuery connector supports both types in the following ways.

### Managed tables

When you create a managed table using the `CREATE TABLE` statement, the connector creates both
the table metadata in the Hive Metastore and a new table in BigQuery with the same schema.

Here's an example:

```sql
CREATE TABLE mytable (word_count BIGINT, word STRING)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.project'='myproject',
    'bq.dataset'='mydataset',
    'bq.table'='mytable'
);
```

When you drop a managed table using the `DROP TABLE` statement, the connector drops both the table
metadata from the Hive Metastore and the BigQuery table (including all of its data).

### External tables

When you create an external table using the `CREATE EXTERNAL TABLE` statement, the connector only
creates the table metadata in the Hive Metastore. It assumes that the corresponding table in
BigQuery already exists.

Here's an example:

```sql
CREATE EXTERNAL TABLE mytable (word_count BIGINT, word STRING)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.project'='myproject',
    'bq.dataset'='mydataset',
    'bq.table'='mytable'
);
```

When you drop an external table using the `DROP TABLE` statement, the connector only drops the table
metadata from the Hive Metastore. The corresponding BigQuery table remains unaffected.

## Partitioning

As Hive's partitioning and BigQuery's partitioning inherently work in different ways, the Hive
`PARTITIONED BY` clause is not supported. However, you can still leverage BigQuery's partitioning by
using table properties. Two types of partitioning are currently supported: time-unit column
partitioning and ingestion time partitioning.

### Time-unit column partitioning

You can partition a table on a `DATE` or `TIMESTAMP` column. When you write data to the table,
BigQuery automatically puts the data into the correct partition based on the values in the column.

For `TIMESTAMP` columns, the partitions can have either hourly, daily, monthly, or yearly
granularity. For `DATE` columns, the partitions can have daily, monthly, or yearly granularity.
Partitions boundaries are based on UTC time.

To create a table partitioned by a time-unit column, you must set the `bq.time.partition.field`
table property to the column's name.

Here's an example:

```sql
CREATE TABLE mytable (int_val BIGINT, ts TIMESTAMP)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.project'='myproject',
    'bq.dataset'='mydataset',
    'bq.table'='mytable',
    'bq.time.partition.field'='ts',
    'bq.time.partition.type'='MONTH'
);
```

Check out the official BigQuery documentation about [Time-unit column partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables)
to learn more.

### Ingestion time partitioning

When you create a table partitioned by ingestion time, BigQuery automatically assigns rows to
partitions based on the time when BigQuery ingests the data. You can choose hourly, daily, monthly,
or yearly boundaries for the partitions. Partitions boundaries are based on UTC time.

An ingestion-time partitioned table also has two pseudo columns:

- `_PARTITIONTIME`: ingestion time for each row, truncated to the partition boundary (such as hourly
  or daily).
- `_PARTITIONDATE`: UTC date corresponding to the value in the `_PARTITIONTIME` pseudo column.

To create a table partitioned by ingestion time, you must set the `bq.time.partition.type` table
property to the partition boundary of your choice (`HOUR`, `DAY`, `MONTH`, or `YEAR`).

Here's an example:

```sql
CREATE TABLE mytable (int_val BIGINT)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.project'='myproject',
    'bq.dataset'='mydataset',
    'bq.table'='mytable',
    'bq.time.partition.type'='DAY'
);
```

Check out the official BigQuery documentation about [Ingestion time partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables#ingestion_time)
to learn more.

## Clustering

As Hive's clustering and BigQuery's clustering inherently work in different ways, the Hive
`CLUSTERED BY` clause is not supported. However, you can still leverage BigQuery's clustering by
setting the `bq.clustered.fields` table property to a comma-separated list of the columns to cluster
the table by.

Here's an example:

```sql
CREATE TABLE mytable (int_val BIGINT, text STRING, purchase_date DATE)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.project'='myproject',
    'bq.dataset'='mydataset',
    'bq.table'='mytable',
    'bq.clustered.fields'='int_val,text'
);
```

Check out the official BigQuery documentation about [Clustering](https://cloud.google.com/bigquery/docs/clustered-tables)
to learn more.

## Tables properties

You can use the following properties in the `TBLPROPERTIES` clause when you create a new table:

| Property                           | Description                                                                                                                                                                                                                                                       |
|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bq.dataset`                       | Always required. BigQuery dataset name (Optional if the hive database name matches the BQ dataset name)                                                                                                                                                           |
| `bq.table`                         | Always required. BigQuery table name (Optional if hive the table name matches the BQ table name)                                                                                                                                                                  |
| `bq.project`                       | Always required. Your project id                                                                                                                                                                                                                                  |
| `bq.time.partition.type`           | Time partitioning granularity. Possible values: `HOUR`, `DAY`, `MONTH`, `YEAR`                                                                                                                                                                                    |
| `bq.time.partition.field`          | Name of a `DATE` or `TIMESTAMP` column to partition the table by                                                                                                                                                                                                  |
| `bq.time.partition.expiration.ms`  | Partition [expiration time](https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration) in milliseconds                                                                                                                              |
| `bq.time.partition.require.filter` | Set it to `true` to require that all queries on the table [must include a predicate filter]((https://cloud.google.com/bigquery/docs/managing-partitioned-tables#require-filter)) (a `WHERE` clause) that filters on the partitioning column. Defaults to `false`. |
| `bq.clustered.fields`              | Comma-separated list of fields to cluster the table by                                                                                                                                                                                                            |
| `viewsEnabled`                     | Set it to `true` to enable reading views. Defaults to `false`                                                                                                                                                                                                     |
| `materializationProject`           | Project used to temporarily materialize data when reading views. Defaults to the same project as the read view.                                                                                                                                                   |
| `materializationDataset`           | Dataset used to temporarily materialize data when reading views. Defaults to the same dataset as the read view.                                                                                                                                                   |

## Job configuration properties

You can set the following Hive/Hadoop configuration properties in your environment:

| Property                  | Default value       | Description                                                                                                                                                                                        |
|---------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bq.write.method`         | `direct`            | Indicates how to write data to BigQuery. Possible values: `direct` (to directly write to the BigQuery storage API), `indirect` (to stage temprary Avro files to GCS before loading into BigQuery). |
| `bq.temp.gcs.path`        | None                | GCS location for storing temporary Avro files when using the `indirect` write method                                                                                                               |
| `bq.work.dir.parent.path` | `${hadoop.tmp.dir}` | Parent path on HDFS where each job creates its temporary work directory                                                                                                                            |
| `bq.work.dir.name.prefix` | `bq-hive-`          | Prefix used for naming the jobs' temporary directories.                                                                                                                                            |
| `bq.read.data.format`     | `arrow`             | Data format used for reads from BigQuery. Possible values: `arrow`, `avro`.                                                                                                                        |

## Data Type Mapping

| BigQuery  | Hive      | DESCRIPTION                                                                                                                               |
|-----------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| INTEGER   | BIGINT    | Signed 8-byte Integer                                                                                                                     |
| FLOAT     | DOUBLE    | 8-byte double precision floating point number                                                                                             |
| DATE      | DATE      | FORMAT IS YYYY-[M]M-[D]D. The range of values supported for the Date type is 0001-­01-­01 to 9999-­12-­31                                 |
| TIMESTAMP | TIMESTAMP | Represents an absolute point in time since Unix epoch with millisecond precision (on Hive) compared to Microsecond precision on Bigquery. |
| BOOLEAN   | BOOLEAN   | Boolean values are represented by the keywords TRUE and FALSE                                                                             |
| STRING    | STRING    | Variable-length character data                                                                                                            |
| BYTES     | BINARY    | Variable-length binary data                                                                                                               |
| REPEATED  | ARRAY     | Represents repeated values                                                                                                                |
| RECORD    | STRUCT    | Represents nested structures                                                                                                              |

## Execution engines

The BigQuery storage handler supports both the MapReduce and Tez execution engines. Tez is recommended for better
performance -- you can use it by setting the `hive.execution.engine=tez` configuration property.

## Column Pruning

Since BigQuery is [backed by a columnar datastore](https://cloud.google.com/blog/big-data/2016/04/inside-capacitor-bigquerys-next-generation-columnar-storage-format),
it can efficiently stream data without reading all columns.

Column pruning is currently supported only with the Tez engine.

## Predicate Filtering

The Storage API supports arbitrary pushdown of predicate filters. To enable predicate pushdown ensure
`hive.optimize.ppd` is set to `true`.  Filters on all primitive type columns will be pushed to storage layer improving
the performance of reads. Predicate pushdown is not supported on complex types such as arrays and structs.
For example - filters like `address.city = "Sunnyvale"` will not get pushdown to Bigquery.

## Reading From BigQuery Views

The connector has preliminary support for reading from [BigQuery views](https://cloud.google.com/bigquery/docs/views-intro).
Please note there are a few caveats:

* The Storage Read API operates on storage directly, so the API cannot be used to read logical or materialized views. To
  get around this limitation, the connector materializes the views into temporary tables before it can read them. This
  materialization process can affect overall read performance and incur additional costs to your BigQuery bill.
* By default, the materialized views are created in the same project and dataset. Those can be configured by the
  optional `materializationProject` and `materializationDataset` Hive configuration properties or
  table properties, respectively.
* As mentioned in the [BigQuery documentation](https://cloud.google.com/bigquery/docs/writing-results#temporary_and_permanent_tables),
  the `materializationDataset` should be in same location as the view.
* Reading from views is **disabled** by default. In order to enable it, set the `viewsEnabled` configuration
  property to `true`.

## Known issues and limitations

1. The `TINYINT`, `SMALLINT`, and `INT`/`INTEGER` Hive types are not supported. Instead, use the `BIGINT` type, which
   corresponds to BigQuery's `INT64` type. Similarly, the `FLOAT` Hive type is not supported. Instead, use the `DOUBLE`
   type, which corresponds to BigQuery's `FLOAT64` type.
2. Ensure that the table exists in BigQuery and column names are always lowercase.
3. A `TIMESTAMP` column in hive is interpreted to be timezone-less and stored as an offset from the UNIX epoch with
   milliseconds precision. To display in human-readable format, use the `from_unix_time` UDF:

   ```sql
   from_unixtime(cast(cast(<timestampcolumn> as bigint)/1000 as bigint), 'yyyy-MM-dd hh:mm:ss')
   ```
4. If a write job fails while using the Tez execution engine and the `indirect` write method, then the temporary avro
   files might not be automatically cleaned up from the GCS bucket. The MR execution engine does not have that
   limitation. The temporary files are always cleaned up when the job is successful, regardless of the execution engine
   in use.

## Missing features

The following features are not available yet but are planned or are under development:

- `UPDATE`, `MERGE`, and `DELETE` statements.
- `ALTER TABLE` statements.

Your feedback and contributions are welcome for developing those new features.

## Development

### Code formatting

To standardize the code's format, run [Spotless](https://github.com/diffplug/spotless) like so:

```sh
./mvnw spotless:apply
```

### Running the tests

You must use Java version 8, as it's the version that Hive itself uses. Make sure that `JAVA_HOME` points to the Java
8's base directory.

Create a service account and give the following roles in your project:

- BigQuery Data Owner
- BigQuery Job User
- BigQuery Read Session User
- Storage Admin

Download a JSON private key for the service account, and set the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable:

```sh
GOOGLE_APPLICATION_CREDENTIALS=<path/to/your/key.json>
```

To run the integration tests:

```sh
./mvnw verify --projects shaded-dependencies,connector -Dit.test="IntegrationTests"
```

To run a specific test method:

```sh
./mvnw verify --projects shaded-dependencies,connector -Dit.test="IntegrationTests#testInsertTez"
```

To debug the tests, add the `-Dmaven.failsafe.debug` property:

```sh
./mvnw verify -Dmaven.failsafe.debug --projects shaded-dependencies,connector -Dit.test="IntegrationTests"
```

... then run a remote debugger in IntelliJ at port 5005.

Read more about debugging with FailSafe here: https://maven.apache.org/surefire/maven-failsafe-plugin/examples/debugging.html
