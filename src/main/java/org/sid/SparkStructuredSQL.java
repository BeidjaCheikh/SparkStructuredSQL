package org.sid;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class SparkStructuredSQL {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession ss = SparkSession
                .builder()
                .appName("Company Incidents")
                .master("local[*]")
                .getOrCreate();

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                        new StructField("titre", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("description", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("service", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("date", DataTypes.StringType, false, Metadata.empty()),
                }
        );

        String csvFolderPath = "src/main/resources";

        Dataset<Row> lines = ss.readStream()
                .option("header", true)
                .schema(schema)
                .csv(csvFolderPath);

        StreamingQuery query = lines.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
