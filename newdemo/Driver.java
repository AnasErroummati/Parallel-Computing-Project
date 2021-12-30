package newdemo;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.util.StatCounter;

import java.util.Iterator;
import java.util.List;

public class Driver {
    private static final String MASTER_URL = "spark://10.10.10.10:7070";
    private static final String APP_NAME = "Rating";

    public static void main(String[] args) {
        int i=0;
        // N Program Constant
        int N = 3;


        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER_URL);
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<rating> ratings = null; // To do

        JavaRDD<rating> ratingRDD = sc.parallelize(ratings);

        JavaPairRDD<String, Integer> indexedRatingRDD = ratingRDD
                                .mapToPair(rating -> new Tuple2<>(rating.getProductId(), rating.getStars()));
        
        JavaPairRDD<String, Integer> ratingSumByProductRDD = indexedRatingRDD.reduceByKey((a, b) -> a + b);

        
        JavaPairRDD<String, Integer> ratingCountByProductRDD = indexedRatingRDD
                                .mapToPair(indexedRating -> new Tuple2<>(indexedRating._1, 1)).reduceByKey((a, b) -> a + b);
         
        JavaPairRDD<String, Tuple2<Integer, Integer>> ratingSumAndCountByProductRDD = ratingSumByProductRDD
                                .join(ratingCountByProductRDD);
                
        JavaPairRDD<String, Double> averageRatingByProductRDD = ratingSumAndCountByProductRDD
                                .mapToPair(sumAndCountByProduct -> new Tuple2<>(sumAndCountByProduct._1,
                                        (double) sumAndCountByProduct._2._1 / sumAndCountByProduct._2._2));
        //Aprecaited Products

        JavaPairRDD<String, Double> ApraciatedProductRDD = averageRatingByProductRDD.filter(averageRatingByProduct -> averageRatingByProduct._2 >= 4);
        
        List<Tuple2<String, Double>> ApraciatedProduct = ApraciatedProductRDD.collect();

        Iterator<Tuple2<String, Double>> ProductIterator = ApraciatedProduct.iterator();

        System.out.println("List Of Appreciated Products IDs");

        while (ProductIterator.hasNext()) {
            System.out.println(ProductIterator.next()._1);
        }

        //N least Rated Products

        List<Tuple2<String, Double>> LeastNRatedP = averageRatingByProductRDD.takeOrdered(N); //By default the function works assendenly, It will take teh N first least numbers

        Iterator<Tuple2<String, Double>> LeastRatedProducts = LeastNRatedP.iterator();

        System.out.println("List Of N Rated Products IDs");

        while (i<N){
            if(LeastRatedProducts.hasNext()){
                System.out.println(LeastRatedProducts.next()._1);
                i=i+1;
            }
        }

        //Mean And Standard deviation

        JavaDoubleRDD JDRDD = averageRatingByProductRDD.mapToDouble(averageRatingByProduct -> { return averageRatingByProduct._2; });

        double Mean = JDRDD.mean();
        System.out.printf("Mean: %f C \n", Mean);

        double stdev = JDRDD.stdev();
        System.out.printf("Standard Deviation: %f C\n", stdev);



    }



}