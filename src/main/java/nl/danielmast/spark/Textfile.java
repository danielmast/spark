package nl.danielmast.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Textfile {
    public static void main(String[] args) {
        // Based on the video: https://youtu.be/tDVPcqGpEnM
        // and this article: https://spark.apache.org/docs/0.8.1/java-programming-guide.html

        long start = System.currentTimeMillis();

        String file = "data.txt";

        SparkConf conf = new SparkConf().setAppName("Read text file").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);

        System.out.println("SparkContext has been setup " + (System.currentTimeMillis() - start));

        JavaRDD<String> lines = sc.textFile("data.txt", 1).toJavaRDD();

        System.out.println("Text file has been loaded " + (System.currentTimeMillis() - start));

        JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.split(" ")).iterator());

        System.out.println("FlatMap is finished " + (System.currentTimeMillis() - start));

        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)
        );

        System.out.println("Pairs have been created " + (System.currentTimeMillis() - start));

        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(Integer::sum);

        System.out.println("WordCount is computed " + (System.currentTimeMillis() - start));

        JavaPairRDD<Integer, String> sorted = wordCount.mapToPair(Tuple2::swap).sortByKey(false);

        System.out.println("WordCount is sorted" + (System.currentTimeMillis() - start));

        List<Tuple2<Integer, String>> tuples = sorted.collect();

        System.out.println("Collect is done " + (System.currentTimeMillis() - start));

        tuples.forEach(System.out::println);
    }
}
