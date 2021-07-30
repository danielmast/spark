package nl.danielmast.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLOutput;

public class IMDB {
    public static void main(String[] args) {
        // File can be downloaded at: https://datasets.imdbws.com/
        String file = "/Users/daniel/Downloads/title.basics.tsv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Build a DataFrame from Scratch")
                .master("local[*]")
                .getOrCreate();

        long start = System.currentTimeMillis();
        Dataset<Row> df = spark.read().format("csv")
                .option("sep", "\t")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(file);
        System.out.println(System.currentTimeMillis() - start);

        df.printSchema();

        // Show movies with 'Shawshank' in the title
        start = System.currentTimeMillis();
        Dataset<Row> shawshankDf = df.filter(org.apache.spark.sql.functions.col("primaryTitle").contains("Shawshank"));
        System.out.println(System.currentTimeMillis() - start);
        shawshankDf.show();
        System.out.println(System.currentTimeMillis() - start);
    }
}
