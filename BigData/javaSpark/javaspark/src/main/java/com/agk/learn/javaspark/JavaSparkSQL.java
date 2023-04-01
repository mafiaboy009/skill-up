package com.agk.learn.javaspark;

import lombok.Data;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.*;
import org.codehaus.janino.Java;
import scala.Int;
import scala.None;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.Data;
import scala.Tuple4;
import scala.Tuple5;

public class JavaSparkSQL {//implements Serializable{
    public @Data static class Person implements Serializable {
        private String name;
        private long age;

    }

    public static class SuperheroNode implements Serializable{
        @Getter private final Integer superheroId;
        @Setter @Getter private List<SuperheroNode> superheroConnections;
        @Setter @Getter private Integer distanceFromMainSuperhero;
        @Setter @Getter private String visitColour;

        public SuperheroNode(Integer id, List<SuperheroNode> neighbors) {
            this.superheroId = id;
            this.superheroConnections = neighbors;
            this.distanceFromMainSuperhero = 9999;
            this.visitColour = "white";
        }

        public SuperheroNode(Integer id, List<SuperheroNode> neighbors, Integer distance, String colour) {
            this.superheroId = id;
            this.superheroConnections = neighbors;
            this.distanceFromMainSuperhero = distance;
            this.visitColour = colour;
        }

        // Override equals and hashCode methods for efficient operations in Spark
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SuperheroNode)) {
                return false;
            }
            SuperheroNode other = (SuperheroNode) obj;
            return superheroId == other.superheroId;
        }

        @Override
        public int hashCode() {
            return superheroId;
        }
    }

    private static Integer findMaxValueFromJavaPairRDD(JavaPairRDD<Integer,Integer> inJavaPairRdd){

        JavaPairRDD<Integer,Integer> swapped = inJavaPairRdd
                .mapToPair(Tuple2::swap);

        return swapped
                .reduce( (a,b) -> a._1() > b._1() ? a : b )
                ._1();
    }

    private static Integer findMinValueFromJavaPairRDD(JavaPairRDD<Integer,Integer> inJavaPairRdd){
        JavaPairRDD<Integer,Integer> swapped = inJavaPairRdd
                .mapToPair(Tuple2::swap);

        return swapped
                .reduce( (a,b) -> a._1() < b._1() ? a : b )
                ._1();
    }
    public static void main(String[] args){

/*        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Java Spark RDD app");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);*/

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java SparkSQL App")
                .master("local")
                .getOrCreate();

//        friendsByAgeUsingSQLQuery(sparkSession);
//        friendsByAgeUsingBuiltInSQLCommands(sparkSession);
//        wordCountUsingDataset(sparkSession);
//        wordCountUsingDatasetAndRDD(sparkSession);
//        weatherDataFilter(sparkSession);
//        customerOrder(sparkSession);
//        ratingCounter(sparkSession);
//        mostPopularSuperHero(sparkSession);
//        mostPopularSuperHeroRDD(sparkSession);
//        leastPopularSuperHeroRDD(sparkSession);
//        findDegreeOfSeparationBetweenSuperheroes(sparkSession);
        findSimilarMovies(sparkSession);


        sparkSession.stop();
    }

    private static void findSimilarMovies(SparkSession sparkSession){
        // Create schema when reading u.item
        StructType moviesNamesSchema = new StructType()
                .add("movieID", IntegerType, true)
                .add("movieTitle", DataTypes.StringType, true);

        // Create schema when reading u.data
        StructType moviesSchema = new StructType()
                .add("userID", IntegerType, true)
                .add("movieID", IntegerType, true)
                .add("rating", IntegerType, true)
                .add("timestamp", DataTypes.LongType, true);
    }

    private static void findDegreeOfSeparationBetweenSuperheroes(SparkSession sparkSession) {
        System.out.println("+++++ Java Spark SQL : using RDD : Degree of Separation between Superheroes +++++");

        final Scanner scanner1 = new Scanner(System.in);
        System.out.print("Enter superhero 1: ");
        final Integer superhero1 = Integer.parseInt( scanner1.nextLine() );

        final Scanner scanner2 = new Scanner(System.in);
        System.out.print("Enter superhero 2: ");
        final Integer superhero2 = Integer.parseInt( scanner2.nextLine() );

        System.out.println("Finding distance between " + superhero1 + " and " + superhero2);

        Integer theDistance = 9999;

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> superHeroGraph = sparkContext.textFile(".\\src\\main\\resources\\marvelGraph.txt");

        JavaRDD<Tuple5<Integer, List<Integer>, Integer, String, Integer>> superHeroData = superHeroGraph
                .map(data -> {
                    String[] splits = data.split("\\s+");
                    Integer HeroId = Integer.parseInt(splits[0].trim());
                    List<String> strNeighbours = new ArrayList<>(Arrays.asList(splits).subList(1, splits.length));
                    List<Integer> Neighbours = new ArrayList<>();
                    for (String str : strNeighbours) {
                        Neighbours.add(Integer.parseInt(str));
                    }
                    int distance = 9999;
                    String colour = "white";
                    if (HeroId.equals(superhero1)) {
                        distance = 0;
                        colour = "gray";
                    }
                    Integer numberOfConnections = Neighbours.size();
                    return new Tuple5<>(HeroId, Neighbours, distance, colour, numberOfConnections);
                });

        JavaPairRDD<Integer, Tuple5<Integer, List<Integer>, Integer, String, Integer>> superheroGraph = superHeroData
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple))
                .reduceByKey((x, y) -> {
                    List<Integer> combinedList = new ArrayList<>(x._2());
                    combinedList.addAll(y._2());
                    return new Tuple5<>(x._1(), combinedList, x._3(), x._4(), x._5());
                })
                .sortByKey();

        final Integer numberOfIterations = 10;

        for(Integer i = 1; i < numberOfIterations; i++ ){
            System.out.println("Implementation pending " + i);
            // use accumulator
            /*
            import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CollectionAccumulatorExample {

    public static void main(String[] args) {

        // Create a Spark configuration object
        SparkConf conf = new SparkConf().setAppName("Collection Accumulator Example").setMaster("local[*]");

        // Create a Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create an accumulator variable
        Accumulator<List<String>> accumulator = sc.accumulator(new ArrayList<String>(), new ListAccumulatorParam());

        // Create an RDD
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello", "world", "from", "spark"));

        // Add each element of the RDD to the accumulator
        rdd.foreach(x -> accumulator.add(Arrays.asList(x)));

        // Print the value of the accumulator
        System.out.println("Accumulated List: " + accumulator.value());

        // Stop the Spark context
        sc.stop();
    }
}

// AccumulatorParam implementation for List<String>
class ListAccumulatorParam implements AccumulatorParam<List<String>> {

    @Override
    public List<String> addInPlace(List<String> r1, List<String> r2) {
        r1.addAll(r2);
        return r1;
    }

    @Override
    public List<String> zero(List<String> initialValue) {
        return new ArrayList<String>();
    }

    @Override
    public List<String> addAccumulator(List<String> r1, List<String> r2) {
        r1.addAll(r2);
        return r1;
    }
}

             */
        }

        System.out.println("====++++****$$$$ The distance between " + superhero1 + " and " + superhero2 + " is " + theDistance);
    }

    private static void findDegreeOfSeparationBetweenSuperheroesUsingLocalAndRdd(SparkSession sparkSession) {
        System.out.println("+++++ Java Spark SQL : using Local Variables and RDD : Degree of Separation between Superheroes +++++");

        final Scanner scanner1 = new Scanner(System.in);
        System.out.print("Enter superhero 1: ");
        final Integer mainSuperhero = Integer.parseInt( scanner1.nextLine() );

        final Scanner scanner2 = new Scanner(System.in);
        System.out.print("Enter superhero 2: ");
        final Integer stopSuperhero = Integer.parseInt( scanner2.nextLine() );

        System.out.println("Finding distance between " + mainSuperhero + " and " + stopSuperhero);

/*        Integer mainSuperhero = 1; // Id of Captain America 859
        Integer stopSuperhero = 6486; // 3994-1; 602-2*/

        /*    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter superhero 1:");
            mainSuperhero = Integer.parseInt(br.readLine());
            System.out.println("Enter superhero 2:");
            stopSuperhero = Integer.parseInt(br.readLine());*/


        Integer theDistance = 9999;

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> superHeroGraph = sparkContext.textFile(".\\src\\main\\resources\\marvelGraph.txt");

        JavaRDD<Tuple5<Integer, List<Integer>, Integer, String, Integer>> superHeroData = superHeroGraph
                .map(data -> {
                    String[] splits = data.split("\\s+");
                    Integer HeroId = Integer.parseInt(splits[0].trim());
                    List<String> strNeighbours = new ArrayList<>(Arrays.asList(splits).subList(1, splits.length));
                    List<Integer> Neighbours = new ArrayList<>();
                    for (String str : strNeighbours) {
                        Neighbours.add(Integer.parseInt(str));
                    }
                    int distance = 9999;
                    String colour = "white";
                    if (HeroId.equals(mainSuperhero)) {
                        distance = 0;
                        colour = "gray";
                    }
                    Integer numberOfConnections = Neighbours.size();
                    return new Tuple5<>(HeroId, Neighbours, distance, colour, numberOfConnections);
                });

        JavaPairRDD<Integer, Tuple5<Integer, List<Integer>, Integer, String, Integer>> superheroGraph = superHeroData
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple))
                .reduceByKey((x, y) -> {
                    List<Integer> combinedList = new ArrayList<>(x._2());
                    combinedList.addAll(y._2());
                    return new Tuple5<>(x._1(), combinedList, x._3(), x._4(), x._5());
                })
                .sortByKey();

        //superheroGraph.saveAsTextFile("SuperHeroGraph");

        Tuple5<Integer, List<Integer>, Integer, String, Integer> currentSuperheroNode = superheroGraph
                .lookup(mainSuperhero)
                .get(0);

        if (stopSuperhero.equals(currentSuperheroNode._1())) {
            theDistance = 0;
        } else {

            Queue<Tuple5<Integer, List<Integer>, Integer, String, Integer>> superheroNodeQueue = new LinkedList<>();
            superheroNodeQueue.add(currentSuperheroNode);

            List<Tuple2<Integer, Tuple5<Integer, List<Integer>, Integer, String, Integer>>> superheroNodesList = new ArrayList<>();
            superheroNodesList.add(new Tuple2<>(currentSuperheroNode._1(),
                    new Tuple5<>(currentSuperheroNode._1(), currentSuperheroNode._2(), 0, "black", currentSuperheroNode._5())));

            while (!superheroNodeQueue.isEmpty()) {

                Tuple5<Integer, List<Integer>, Integer, String, Integer> currentSuperheroNode1 = superheroNodeQueue.poll();
                // superheroNodeList has latest information
                // always fetch updated node information from superherNodesList;
                for (Tuple2<Integer, Tuple5<Integer, List<Integer>, Integer, String, Integer>> t1 : superheroNodesList) {
                    if (Objects.equals(t1._1, currentSuperheroNode1._1())) {
                        currentSuperheroNode1 = t1._2;
                        break;
                    }
                }

                for (Integer connId : currentSuperheroNode1._2()) {
                    boolean nodeFoundInList = false;
                    Tuple5<Integer, List<Integer>, Integer, String, Integer> connSuperheroNode = null;
                    int indexOfNode = -1;

                    for (Tuple2<Integer, Tuple5<Integer, List<Integer>, Integer, String, Integer>> t1 : superheroNodesList) {
                        if (Objects.equals(t1._1, connId)) {
                            nodeFoundInList = true;
                            connSuperheroNode = t1._2;
                            break;
                        }
                    }

                    indexOfNode = superheroNodesList.indexOf(new Tuple2<>(connId, connSuperheroNode));

                    if (!nodeFoundInList || indexOfNode == -1) {
                        connSuperheroNode = superheroGraph
                                .lookup(connId)
                                .get(0);

                        superheroNodesList
                                .add(new Tuple2<>(connSuperheroNode._1(), connSuperheroNode));

                        for (Tuple2<Integer, Tuple5<Integer, List<Integer>, Integer, String, Integer>> t1 : superheroNodesList) {
                            if (Objects.equals(t1._1, connId)) {
                                connSuperheroNode = t1._2;
                                break;
                            }
                        }
                    }

                    indexOfNode = superheroNodesList.indexOf(new Tuple2<>(connId, connSuperheroNode));

                    int distance = 9999;
                    String newColour = "";

                    if (connSuperheroNode._4().equals("black")) {
                        continue;
                    } else if (connSuperheroNode._4().equals("white")) {
                        distance = currentSuperheroNode1._3() + 1;
                        newColour = "gray";
                        superheroNodesList
                                .set(indexOfNode, new Tuple2<>(connSuperheroNode._1(),
                                        new Tuple5<>(connSuperheroNode._1(), connSuperheroNode._2(), distance, newColour, connSuperheroNode._5())));
                    } else if (connSuperheroNode._4().equals("gray")) {
                        distance = currentSuperheroNode1._3() + 1;
                        newColour = "black";
                        superheroNodesList
                                .set(indexOfNode, new Tuple2<>(connSuperheroNode._1(),
                                        new Tuple5<>(connSuperheroNode._1(), connSuperheroNode._2(), distance, newColour, connSuperheroNode._5())));
                    }

                    superheroNodeQueue.add(connSuperheroNode);

                    if (connId.equals(stopSuperhero)) {
                        theDistance = distance;
                        superheroNodeQueue.clear();
                        break;
                    }
                }
            }
        }
        System.out.println("====++++****$$$$ The distance between " + mainSuperhero + " and " + stopSuperhero + " is " + theDistance);
    }

    private static void leastPopularSuperHeroRDD(SparkSession sparkSession){
        System.out.println("+++++ Java Spark SQL : using Dataset and RDD : Least Popular Super Hero +++++");

        JavaSparkContext sparkContext = new JavaSparkContext( sparkSession.sparkContext() );

        JavaRDD<String> superHeroGraph = sparkContext.textFile(".\\src\\main\\resources\\marvelGraph.txt");

        JavaPairRDD<Integer, Integer> superHeroIdName = superHeroGraph
                .mapToPair( data -> {
                    String[] splits = data.split("\\s+");
                    Integer HeroId = Integer.parseInt(splits[0].trim());
                    Integer Count = splits.length - 1;
                    return new Tuple2<>(HeroId, Count);
                })
                .reduceByKey(Integer::sum)
                .sortByKey();

        /*
        List<Tuple2<Integer,Integer>> superHeroCollection = superHeroIdName.collect();
        superHeroCollection.forEach(System.out::println);
        */

        Integer minValue = findMinValueFromJavaPairRDD(superHeroIdName);

        System.out.println("The min value: " + minValue);

        JavaPairRDD<Integer, Integer> filtered = superHeroIdName
                .filter(pair -> pair._2().equals(minValue));

        JavaRDD<Integer> keys = filtered.keys();
        List<Integer> keyList = keys.collect();
        keyList.forEach(System.out::println);

        JavaRDD<String> superHeroIdNames = sparkContext
                .textFile(".\\src\\main\\resources\\marvelNames.txt");

        JavaPairRDD<Integer,String> superHeroIdNameLookup = superHeroIdNames.mapToPair(
                data -> {
                    String[] splits = data.split("\"");
                    if (splits.length > 1) {
                        Integer HeroId = Integer.parseInt(splits[0].trim());
                        String HeroName = splits[1].trim();
                        return new Tuple2<>(HeroId, HeroName);
                    }
                    return new Tuple2<>(0, "");
                });

        // create an empty list;
        // never use null to initialize a variable
        List<String> theLeastSuperHero = new ArrayList<>();
        for( Integer key: keyList ){
            //theLeastSuperHero.addAll(tempList.parallelStream().filter(Objects::nonNull).collect(Collectors.toList()));
            theLeastSuperHero.addAll(superHeroIdNameLookup.lookup(key));
        }
        System.out.println("List of Least Popular Superheroes");
        theLeastSuperHero.forEach(System.out::println);
        System.out.println("One of the least popular superhero is " + theLeastSuperHero.get(0));

        /*
        List<Tuple2<Integer,String>> superHeroIdNameLookupCollection = superHeroIdNameLookup.collect();
        superHeroIdNameLookupCollection.forEach(System.out::println);
        */

        //List<String> theSuperHero = superHeroIdNameLookup.lookup(keys.first());
        //System.out.println("One of the least popular superhero is " + theSuperHero.get(0));
    }
    private static void mostPopularSuperHeroRDD(SparkSession sparkSession){
        System.out.println("+++++ Java Spark SQL : using Dataset and RDD : Most Popular Super Hero +++++");

        JavaSparkContext sparkContext = new JavaSparkContext( sparkSession.sparkContext() );

        JavaRDD<String> superHeroGraph = sparkContext.textFile(".\\src\\main\\resources\\marvelGraph.txt");

        JavaPairRDD<Integer, Integer> superHeroIdName = superHeroGraph
                .mapToPair( data -> {
                    String[] splits = data.split("\\s+");
                    Integer HeroId = Integer.parseInt(splits[0].trim());
                    Integer Count = splits.length - 1;
                    return new Tuple2<>(HeroId, Count);
                })
                .reduceByKey(Integer::sum)
                .sortByKey();

        /*List<Tuple2<Integer,Integer>> superHeroCollection = superHeroIdName.collect();
        superHeroCollection.forEach(System.out::println);*/

        Integer maxValue = findMaxValueFromJavaPairRDD(superHeroIdName);

        System.out.println("The max value: " + maxValue);

        JavaPairRDD<Integer, Integer> filtered = superHeroIdName
                .filter(pair -> pair._2().equals(maxValue));

        JavaRDD<Integer> keys = filtered.keys();
/*
        List<Integer> keyList = keys.collect();
        keyList.forEach(System.out::println);
*/

        JavaRDD<String> superHeroIdNames = sparkContext
                .textFile(".\\src\\main\\resources\\marvelNames.txt");

        JavaPairRDD<Integer,String> superHeroIdNameLookup = superHeroIdNames.mapToPair(
                data -> {
                    String[] splits = data.split("\"");
                    if (splits.length > 1) {
                        Integer HeroId = Integer.parseInt(splits[0].trim());
                        String HeroName = splits[1].trim();
                        return new Tuple2<>(HeroId, HeroName);
                    }
                    return new Tuple2<>(0, "");
                });

/*        List<Tuple2<Integer,String>> superHeroIdNameLookupCollection = superHeroIdNameLookup.collect();
        superHeroIdNameLookupCollection.forEach(System.out::println);*/

        List<String> theSuperHero = superHeroIdNameLookup.lookup(keys.first());
        System.out.println("Our Most popular super hero is " + theSuperHero.toString());
        theSuperHero.forEach(System.out::println);
    }

    private static void mostPopularSuperHero(SparkSession sparkSession){
        System.out.println("+++++ Java Spark SQL : using Dataset : Most Popular Super Hero +++++");

        StructType superHeroIdNameSchema = new StructType()
                .add("id", IntegerType)
                .add("name",DataTypes.StringType);

        Dataset<Row> superHeroIdNames = sparkSession
                .read()
                .option("delimiter"," ")
                .schema(superHeroIdNameSchema)
                .csv(".\\src\\main\\resources\\marvelNames.txt");

        superHeroIdNames.show();

        Dataset<Row> superHeroGraph = sparkSession
                .read()
                .text(".\\src\\main\\resources\\marvelGraph.txt");

        Dataset<Row> superHeroGraphV2 = superHeroGraph
                .withColumn("heroId", element_at(split(col("value")," "), 1))
                .withColumn("numberOfConnections", size(split(col("value"), " ")).minus(1))
                .groupBy("heroId")
                .agg(sum(col("numberOfConnections")).alias("numberOfConnections"))
                .sort(desc("numberOfConnections"));

        superHeroGraphV2.show();

        Row mostPopularSuperHeroes = superHeroGraphV2.first();

        Dataset<Row> mostPopularHero = superHeroIdNames
                .filter(col("id").equalTo(mostPopularSuperHeroes.getAs("heroId")))
                .select("name");

        Row ourHero = mostPopularHero.first();
        System.out.println(ourHero.toString());
    }

    private static void ratingCounter(SparkSession sparkSession) {
        System.out.println("+++++ Java Spark SQL : Rating Counter +++++");

        StructType movieRawSchema = new StructType()
                .add("custId", IntegerType)
                .add("movieId", IntegerType)
                .add("rating", IntegerType)
                .add("timestamp", IntegerType);

        Dataset<Row> inputFile = sparkSession
                .read()
                .option("delimiter","\t")
                .schema(movieRawSchema)
                .csv(".\\src\\main\\resources\\movieData_rating.space");

        Dataset<Row> ratedDataset = inputFile
                .groupBy("movieId")
                .agg(functions.round(functions.avg("rating"),2))
                .withColumnRenamed("round(avg(rating), 2)","avg_rating")
                .sort(desc("avg_rating"));

        StructType movieSchema = new StructType()
                .add("movieId", IntegerType)
                .add("movieName", DataTypes.StringType)
                .add("releaseDate", DataTypes.StringType)
                .add("emptyString1", DataTypes.StringType)
                .add("movieURL", DataTypes.StringType)
                .add("genre01", IntegerType)
                .add("genre02", IntegerType)
                .add("genre03", IntegerType)
                .add("genre04", IntegerType)
                .add("genre05", IntegerType)
                .add("genre06", IntegerType)
                .add("genre07", IntegerType)
                .add("genre08", IntegerType)
                .add("genre09", IntegerType)
                .add("genre10", IntegerType)
                .add("genre11", IntegerType)
                .add("genre12", IntegerType)
                .add("genre13", IntegerType)
                .add("genre14", IntegerType)
                .add("genre15", IntegerType)
                .add("genre16", IntegerType)
                .add("genre17", IntegerType)
                .add("genre18", IntegerType)
                .add("genre19", IntegerType);

        Dataset<Row> movieData = sparkSession
                .read()
                .option("delimiter","|")
                .schema(movieSchema)
                .csv(".\\src\\main\\resources\\movieData.pdl");

        //create a Map out of movieData dataset, broadcast it and use this map for lookup
        List<Row> movieList = movieData.select("movieId","movieName").collectAsList();

        Map<Integer, String> movieMap = new HashMap<>();

        for( Row row: movieList){
            Integer movieId = row.getAs("movieId");
            String movieName = row.getAs("movieName");
            movieMap.put(movieId,movieName);
        }//map created

        // Print the map using a for-each loop
        /*for (Map.Entry<Integer, String> entry : movieMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }*/

        JavaSparkContext sparkContext = new JavaSparkContext( sparkSession.sparkContext() );
        sparkContext.broadcast(movieMap);//map broadcasted

        // Define UDF to lookup value in the Map
        //UDF1<Integer, String> lookupUDF = (movieMap::get);// DataTypes.StringType);

        // creating a user defined function (UDF) and use it for lookup movieId and get movieName
        sparkSession.udf().register("lookupMovieId", (UDF1<Integer, String>)(movieMap::get), DataTypes.StringType);
        //OR
        // sparkSession.udf().register("lookupMovieId", lookupUDF, DataTypes.StringType);

        // Apply UDF to the id column of the mainDF dataset
        Dataset<Row> resultDF = ratedDataset
                .select( col("movieId"), col("avg_rating"), functions.callUDF("lookupMovieId", ratedDataset.col("movieId")).as("movieName"));

        resultDF
                .write()
                .mode(SaveMode.Overwrite)
                .json(".\\src\\main\\resources\\ratedMovieData");
    }

    private static void customerOrder(SparkSession sparkSession){

        StructType customerSchema = new StructType()
                .add("customerId", IntegerType)
                .add("itemId", IntegerType)
                .add("amountSpent", DataTypes.DoubleType);

        Dataset<Row> customerData = sparkSession
                .read()
                .option("header","false")
                .schema(customerSchema)
                .csv(".\\src\\main\\resources\\customer-orders.csv");

        customerData.show();

        Dataset<Row> resultData = customerData
                .groupBy("customerId")
                .agg(functions.round(functions.sum("amountSpent"),2));

        resultData
                .withColumnRenamed("round(sum(amountSpent), 2)","totalSpent")
                .sort("customerId")
                .show(10000);
    }
    private static void weatherDataFilter(SparkSession sparkSession) {
        System.out.println("+++++ SparkSQL : Weather MIN and MAX +++++");

        // Define the schema using StructType and StructField
        StructType schema1 = new StructType(new StructField[] {
                new StructField("stationID", DataTypes.StringType, true, null),
                new StructField("date", IntegerType, true, null),
                new StructField("measure_type", DataTypes.StringType, true, null),
                new StructField("temperature", DataTypes.DoubleType, true, null),
        });

        StructType schema2 = new StructType()
                .add("id", DataTypes.StringType)
                .add("date", IntegerType)
                .add("measure_type", DataTypes.StringType)
                .add("temperature", DataTypes.DoubleType);

        Dataset<Row> dataset = sparkSession
                .read()
                .schema(schema2) // Note: schema1 was giving null pointer exception
                .csv(".\\src\\main\\resources\\weatherData.csv");

        Dataset<Row> filteredDataset = dataset.filter(col("measure_type").equalTo("TMIN"));

        Dataset<Row> cleanDataset = filteredDataset
                .select("id","temperature")
                .groupBy("id")
                .min("temperature");

        cleanDataset.show();

        // renaming a column
        //Dataset<Row> columnRenamed = cleanDataset.withColumnRenamed("min(temperature)", "temperature");

        Dataset<Row> correctedDataset = cleanDataset
                .withColumn("temperature",col("min(temperature)").$div(10)); // you are creating new column

        Dataset<Row> resultDataset = correctedDataset.drop("min(temperature)"); // deleting a column of information
        resultDataset.sort("temperature").show();
    }
    private static void wordCountUsingDatasetAndRDD(SparkSession sparkSession){
        System.out.println("+++++ SparkSQL : Word Count using Dataset and RDD +++++");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> inputFile = sparkContext.textFile(".\\src\\main\\resources\\input.txt");
        JavaRDD<String> words = inputFile
                .flatMap(data -> Arrays.asList(data.split("\\W+")).iterator())
                .map(String::toLowerCase);

        Dataset<String> wordsDS = sparkSession.createDataset(words.rdd(), Encoders.STRING());

        Dataset<Row> wordCount = wordsDS
                .select(col("value"))
                .groupBy("value")
                .count()
                .sort("count");

        wordCount.show(200);
    }
    private static void wordCountUsingDataset(SparkSession sparkSession){
        System.out.println("+++++ SparkSQL : Word Count using Dataset +++++");

        Dataset<Row> input = sparkSession
                .read()
                .text(".\\src\\main\\resources\\input.txt")
                .as("Book");

        //input.show();

        Dataset<Row> words = input
                .select(explode(split(col("value"), "\\W+"))
                        .alias("word"))
                .filter(col("word").notEqual(""))
                .withColumn("word", lower(col("word")));
        //words = words.withColumn("word", lower(col("word")));

        Dataset<Row> wordCounts = words
                .groupBy("word")
                .count()
                .sort("word");

        wordCounts.show();
    }

    private static void friendsByAgeUsingBuiltInSQLCommands(SparkSession sparkSession) {

        System.out.println("+++++ SparkSQL : Built-in SQL Commands: Friends by Age +++++");
        Dataset<Row> dataset = sparkSession
                .read()
                .option("header","true")
                .option("inferSchema","true")
                .csv(".\\src\\main\\resources\\fakefriends.csv");
        //.as(Encoders.bean(Person.class));

        dataset.printSchema();

        dataset.select("name").show();

        //println("Filter out anyone over 21:")
        dataset.filter(col("age").gt(21)).show();

        //println("Group by age:")
        dataset.groupBy("age").count().show();

        dataset.groupBy("age")
                .agg(functions.avg("friends").as("avg_friends"))
                .sort("age")
                .show();

        //println("Make everyone 10 years older:")
        dataset.select(dataset.col("name"), dataset.col("age").plus(10).as("new_age")).show();
        //OR
        //dataset.select(col("name"), col("age").plus(10)).show();

    }
    private static void friendsByAgeUsingSQLQuery(SparkSession sparkSession) {

        System.out.println("+++++ SparkSQL: SQL Query : Friends by Age +++++");
        Dataset<Row> dataset = sparkSession
                .read()
                .option("header","true")
                .option("inferSchema","true")
                .csv(".\\src\\main\\resources\\fakefriends.csv");
        //.as(Encoders.bean(Person.class));

        dataset.printSchema();

        dataset.createOrReplaceTempView("people");

        Dataset<Row> teenagers = sparkSession.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19");

        teenagers.show();
        //OR
        teenagers.foreach(row -> {
            // do something with the row, such as print it to the console
            System.out.println(row);
        });


        //collect should be used very carefully
        //it brings data from every node to master node
        //out of memory error could occur
        Row[] rows = (Row[]) teenagers.collect();

        // iterate over the rows and print out the values of each column
        for (Row row : rows) {
            System.out.println("Column 1: " + row.getAs("name"));
            System.out.println("Column 2: " + row.getAs("age"));
            System.out.println("Column 3: " + row.getAs("friends"));
        }

/*        Encoder<String> stringEncoder = Encoders.STRING();

        Dataset<String> teenagerNamesByIndexDF = teenagers.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(1),
                stringEncoder);

        teenagerNamesByIndexDF.show();*/

    }
}
