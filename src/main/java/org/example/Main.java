package org.example;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .master("local[*]")
                .getOrCreate();

        String fil_unload1 = "C:/Users/koshkinas/Desktop/spark/untitled7/src/main/resources/df_itog1.csv";
        String fil_unload2 = "C:/Users/koshkinas/Desktop/spark/untitled7/src/main/resources/df_itog2.csv";
        Primer1 primer1 = new Primer1();

        primer1.runner(spark,  fil_unload1,  fil_unload2);

        if(!Files.exists(Path.of("fil_unload1")))
        {
            primer1.select(spark, fil_unload1);
        }

        if(!Files.exists(Path.of("fil_unload2")))
        {
            primer1.select(spark, fil_unload2);
        }
    }
}
