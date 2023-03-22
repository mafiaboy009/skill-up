package com.agk.learn.javaspark;

/*
For windows platform
Download winutils.exe from the link https://github.com/cdarlint/winutils
and put it in bin folder at HADOOP_HOME

Scala to Java converter
https://www.javainuse.com/sc2ja
 */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.Collectors;


public class JavaSparkRdd
{
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Java Spark RDD app");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        wordCount(sparkContext);
        ratingCounter(sparkContext);
        friendsByAge(sparkContext);
        weatherDataFilter(sparkContext);
        customerOrder(sparkContext);

        sparkContext.close();
    }
    private static void customerOrder(JavaSparkContext sparkContext) {
        System.out.println("+++++ Customer Order +++++");
        JavaRDD<String> inputFile = sparkContext.textFile(".\\src\\main\\resources\\customer-orders.csv");
        JavaPairRDD<String, Integer> customerIdPrice = inputFile
                .mapToPair( data -> {
                    String[] fields = data.split(",");
                    Integer customerId = Integer.parseInt(fields[0]);
                    Double customerSpent = Double.parseDouble(fields[2]);
                    return new Tuple2<>(customerId,customerSpent);
                })
                .reduceByKey((x,y)->(x+y))
                .mapToPair( data -> new Tuple2<>(data._2.toString(),data._1))
                .sortByKey();
        List<Tuple2<String,Integer>> customerOrder = customerIdPrice.collect();
        customerOrder.forEach(System.out::println);
    }
    private static void weatherDataFilter(JavaSparkContext sparkContext){
        System.out.println("+++++ Weather Filter +++++");
        JavaRDD<String> inputFile = sparkContext.textFile(".\\src\\main\\resources\\weatherData.csv");
        JavaRDD<Tuple3<String, String, Double>> parseFields = inputFile.map(line -> {
            String[] splits = line.split(",");
            String stationId = splits[0];
            String entryType = splits[2].trim();
            Double temperature = Double.parseDouble(splits[3]);
            return new Tuple3<>(stationId, entryType, temperature);
            });
        //parseFields.foreach( data -> { System.out.println("stationId= "+data._1() + "; entryType= " + data._2() +"; temperature " + data._3()); });
        JavaRDD<Tuple3<String, String, Double>> filteredData = parseFields.filter(data -> (data._2().equals("TMIN")));
        //filteredData.foreach( data -> { System.out.println("++ stationId= "+data._1() + "; entryType= " + data._2() +"; temperature " + data._3()); });
        JavaPairRDD<String, Double> stationMinTemp = filteredData
                .mapToPair(data -> new Tuple2<>(data._1(), data._3()))
                .reduceByKey(Math::min);
        List<Tuple2<String, Double>> weatherData = stationMinTemp.collect();
        weatherData.forEach(data -> { System.out.println("stationId= "+data._1() + "; temperature= " + data._2()); });

    }
    private static void friendsByAge(JavaSparkContext sparkContext) {
        System.out.println("+++++ Friends by Age +++++");
        JavaRDD<String> inputFile = sparkContext.textFile(".\\src\\main\\resources\\fakefriends-noheader.csv");
        JavaPairRDD<Integer, Integer> parseFields = inputFile.mapToPair(line -> {
            String[] splits = line.split(",");
            int ageKey = Integer.parseInt(splits[2]);
            int numberOfFriendsValue = Integer.parseInt(splits[3]);
            return new Tuple2<>(ageKey,numberOfFriendsValue);
        });
        //Note: keys will not be unique
        //parseFields.foreach(data -> { System.out.println("key="+data._1() + " value=" + data._2()); });
        JavaPairRDD<Integer, Tuple2<Integer,Integer>> ageFriends = parseFields
                .mapValues( data -> new Tuple2<>(data,1))
                .reduceByKey((x,y) -> new Tuple2<>(x._1+y._1, x._2+y._2));
        //ageFriends.foreach(data -> { System.out.println("key="+data._1() + " values=" + data._2._1() + " " + data._2._2()); });
        JavaPairRDD<Integer, Double> avgByAge = ageFriends
                .mapValues( x -> ( new Double(x._1)/(new Double(x._2))))
                .sortByKey();
        //avgByAge.foreach(data -> { System.out.println("key="+data._1() + " value=" + data._2()); });
        List<Tuple2<Integer,Double>> results = avgByAge.collect();
        results.forEach(System.out::println);
    }
    private static void ratingCounter(JavaSparkContext sparkContext) {
        System.out.println("+++++ Rating Counter +++++");
        JavaRDD<String> inputFile = sparkContext.textFile(".\\src\\main\\resources\\u.data");
        JavaRDD<String> ratings = inputFile.map(content -> content.split("\t")[2]);
        Map<String, Long> results = ratings.countByValue();
        List<Tuple2<String, Long>> sortedResults = results
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(x -> new Tuple2<>(x.getKey(), x.getValue()))
                .collect(Collectors.toList());
        sortedResults.forEach(System.out::println);
  }
    private static void wordCount(JavaSparkContext sparkContext) {
        System.out.println("+++++ Word Count +++++");
        JavaRDD<String> inputFile = sparkContext.textFile(".\\src\\main\\resources\\input.txt");
        JavaRDD<String> wordsFromFile = inputFile
                .flatMap(content -> Arrays.asList(content.split("\\W+")).iterator())
                .map(String::toLowerCase);
/*
        Map<String,Long> wordCounts = wordsFromFile.countByValue();
        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }*/
        JavaPairRDD<String, Integer> countData = wordsFromFile
                .mapToPair(t -> new Tuple2<>(t, 1))
                .reduceByKey((x, y) -> (int) x + (int) y)
                .sortByKey();
        //countData.saveAsTextFile();
        List<Tuple2<String,Integer>> result = countData.collect();
        result.forEach(System.out::println);
    }

}