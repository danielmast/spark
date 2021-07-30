package nl.danielmast.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        // got a 'Spark Hello World' working by following: https://mrpowers.medium.com/creating-a-java-spark-project-with-maven-and-junit-614e8be96c3f

        SparkSession spark = SparkSession
                .builder()
                .appName("Build a DataFrame from Scratch")
                .master("local[*]")
                .getOrCreate();


        List<String []> stringAsList = new ArrayList<>();
        stringAsList.add(new String[] { "bar1.1", "bar2.1" });
        stringAsList.add(new String[] { "bar1.2", "bar2.2" });

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Row> rowRDD = sparkContext
                .parallelize(stringAsList)
                .map(RowFactory::create);

        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField("foe1", DataTypes.StringType, false),
                        DataTypes.createStructField("foe2", DataTypes.StringType, false)
                });

        Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();

        Transformations transformations = new Transformations();
        long result = transformations.myCounter(df);
        System.out.println("Count = " + result);

    }
}
