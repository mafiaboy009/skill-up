package com.agk.learn.javaspark;

import lombok.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.List;

public class JavaSparkExtras {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person implements Serializable {
        private Integer id;
        private String name;
        private Integer age;
        private Integer numberOfFriends;
    }

    public static void main(String args[]){

        SparkSession spark = SparkSession.builder()
                .appName("Read CSV with Class Example")
                .master("local[*]")
                .getOrCreate();

        JavaRDD<Person> personRDD = spark.read()
                .option("header", true)
                .csv(".\\src\\main\\resources\\fakefriends.csv")
                .toJavaRDD()
                .map(row -> new Person(Integer.parseInt( row.getString(0)), row.getString(1), Integer.parseInt( row.getString(2)), Integer.parseInt( row.getString(3))));

        List<Person> people = personRDD.collect();

        for (Person person : people) {
            System.out.println("Name: " + person.getName() +
                    ", Age: " + person.getAge() +
                    ", Number of friends: " + person.getNumberOfFriends());
        }

    }

}
