package org.sid;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.count;

public class SparkStructuredSQL {

    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

        SparkSession ss = SparkSession.builder()
                .appName("StreamingIncidents")
                .master("local[*]")
                .getOrCreate();

        // Define the schema for incidents
        StructType incidentSchema = new StructType()
                .add("Id", "integer")
                .add("Titre", "string")
                .add("Description", "string")
                .add("Service", "string")
                .add("Date", "string");

        // Read streaming data from CSV files
        Dataset<Row> stockData = ss.readStream()
                .option("sep", ",")
                .schema(incidentSchema)
                .csv("hdfs://localhost:9000/rep1");

        // Task 1: Display the number of incidents per service continuously
        Dataset<Row> resultDf = stockData.groupBy("Service")
                .agg(count(stockData.col("Service")))
                .filter("Service IS NOT NULL AND Service != 'Service'");

        // Task 2: Display the top two years with the most incidents continuously
        Dataset<Row> resultDf2 = stockData.withColumn("year", stockData.col("Date").substr(0, 4));
        Dataset<Row> incidentsCountByYear = resultDf2.groupBy("year")
                .count()
                .filter("year IS NOT NULL AND year != 'Date'")
                .sort(org.apache.spark.sql.functions.desc("count"))
                .limit(2);

        // Start streaming queries
        StreamingQuery query1 = resultDf.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("8000 milliseconds"))
                .start();

        StreamingQuery query2 = incidentsCountByYear.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("8000 milliseconds"))
                .start();

        // Wait for both queries to terminate
        //query1.awaitTermination();
        query2.awaitTermination();
    }
}
