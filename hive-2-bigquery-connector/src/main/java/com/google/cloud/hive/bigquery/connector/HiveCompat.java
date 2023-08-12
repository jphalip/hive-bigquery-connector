package com.google.cloud.hive.bigquery.connector;

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.DatetimeUtils;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

import java.sql.Timestamp;
import java.time.*;

public class HiveCompat extends HiveCompatBase {

    @Override
    public Object convertHiveTimeUnitToBq(ObjectInspector objectInspector, Object hiveValue, String writeMethod) {
        if (objectInspector instanceof TimestampObjectInspector) {
            TimestampWritable writable;
            if (hiveValue instanceof LazyTimestamp) {
                writable = ((LazyTimestamp) hiveValue).getWritableObject();
            } else {
                writable = (TimestampWritable) hiveValue;
            }
            Timestamp timestamp = writable.getTimestamp();
            if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
                return DatetimeUtils.getEpochMicrosFromHiveTimestamp(timestamp);
            } else {
                return DatetimeUtils.getEncodedProtoLongFromHiveTimestamp(timestamp);
            }
        }
        if (objectInspector instanceof DateObjectInspector) {
            DateWritable writable;
            if (hiveValue instanceof LazyDate) {
                writable = ((LazyDate) hiveValue).getWritableObject();
            } else {
                writable = (DateWritable) hiveValue;
            }
            return new Integer(writable.getDays());
        }

        return null;
    }

    @Override
    public Object convertTimeUnitFromArrow(ObjectInspector objectInspector, Object value, int rowId) {
        if (objectInspector instanceof DateObjectInspector) {
            return new DateWritable(((DateDayVector) value).get(rowId));
        }
        if (objectInspector instanceof TimestampObjectInspector) {
            Timestamp timestamp = DatetimeUtils.getHiveTimestampFromLocalDatetime(((TimeStampMicroVector) value).getObject(rowId));
            return new TimestampWritable(timestamp);
        }
        return null;
    }

    @Override
    public Object convertTimeUnitFromAvro(ObjectInspector objectInspector, Object value) {
        if (objectInspector instanceof DateObjectInspector) {
            return new DateWritable((int) value);
        }
        if (objectInspector instanceof TimestampObjectInspector) {
            LocalDateTime localDateTime = LocalDateTime.parse(((Utf8) value).toString());
            Timestamp timestamp = DatetimeUtils.getHiveTimestampFromLocalDatetime(localDateTime);
            TimestampWritable timestampWritable = new TimestampWritable();
            timestampWritable.setInternal(timestamp.getTime(), timestamp.getNanos());
            return timestampWritable;
        }
        return null;
    }

}