package org.techseeker;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCountDriver implements Serializable {

    public static void main(String[] args) throws InterruptedException {
        SparkConf config = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        SparkContext context = new SparkContext(config);
        new WordCountDriver().wordCount(args);
        //Sleeping to see Spark DAGs for optimization purpose
        Thread.sleep(1000000);
    }

    private void wordCount(String[] args) {
        SparkContext sparkContext = SparkContext.getOrCreate();
        //TODO : Data Structure - RDD ( raw) , Dataframe ( abstracted sql like) , Dataset (user defined object)
        //Difference between these datastructure - when to use what? is it just abstraction or optimized to give performance
        //Different partitioners
        JavaRDD<String> lineRDD = sparkContext.textFile(args[0], 3).toJavaRDD();
        JavaRDD<String> wordsRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                Iterator words = Arrays.stream(line.split(" ")).iterator();
                return words;
            }
        });

        //TODO: try out with RDD<List<String>>

        JavaPairRDD<String, Integer> wordCountPairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> reducedWordCountPairRDD = wordCountPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer existingCount, Integer count) throws Exception {
                return existingCount + count;
            }
        });

        // Tip : ideally store it in file or data store
        List<Tuple2<String, Integer>> wordCountTupleList = reducedWordCountPairRDD.collect();

        //TODO discussed on 21st March
        //reducedWordCountPairRDD.saveAsHadoopDataset(); - sink it to the datastore using this
        //reducedWordCountPairRDD.saveAsxxx; - sink it to the datastore using this
        // Number of connections proportional to the number of nodes *  Executors * number of threads per executor


        //reducedWordCountPairRDD.take(10); - pick few samples in production

        wordCountTupleList.stream().forEach(wordcountTuple -> {
            System.out.println(wordcountTuple._1 + " " + wordcountTuple._2);
        });
    }

}
