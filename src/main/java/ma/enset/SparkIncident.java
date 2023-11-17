package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkIncident {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("TP 5 SPARK SQL").master("local").getOrCreate();

        Dataset<Row> df = ss.read().option("header", true).option("inferSchema",true).csv("incident.csv");

        System.out.println("****** Affichage du dataset ******");
        df.show();

        System.out.println("****** Affichage du nombre d'incidents par services ******");
        df.groupBy(col("service")).count().show();

        System.out.println("****** Affichage des deux années où il a y avait plus d’incidents  ******");
        df.groupBy(col("date")).count().orderBy(col("count").desc()).limit(2).show();

    }
}
