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

import static org.apache.spark.sql.functions.col;

public class SparkStructuredSQL {
    public static void main(String[] args) throws Exception {
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

        // Analyse 1: Afficher d'une manière continue le nombre d'incidents par service
        Dataset<Row> incidentsByService = lines.groupBy("service").count();
        StreamingQuery queryByService = incidentsByService.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Analyse 2: Afficher d'une manière continue les deux années où il y avait le plus d'incidents
        Dataset<Row> incidentsWithYear = lines.withColumn("year", col("date").substr(1, 4));
        Dataset<Row> incidentsByYear = incidentsWithYear.groupBy("year").count();
        Dataset<Row> topTwoYears = incidentsByYear.orderBy(col("count").desc()).limit(2);
        StreamingQuery queryByYear = topTwoYears.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        queryByService.awaitTermination();
        queryByYear.awaitTermination();
    }
}
