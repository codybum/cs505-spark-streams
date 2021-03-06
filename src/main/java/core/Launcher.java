
package core;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class Launcher {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        runStream();

    }

    private static void runStream() {

        try {

            // Create the context with a 1 second batch size
            SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver");
            JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

            // Create an input stream with the custom receiver on target ip:port and count the
            // words in input stream of \n delimited text (eg. generated by 'nc')
            JavaReceiverInputDStream<String> lines = ssc.receiverStream(
                    new JavaCustomReceiver());
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
            JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey((i1, i2) -> i1 + i2);

            wordCounts.print();
            ssc.start();
            ssc.awaitTermination();


        } catch (Exception ex){
            ex.printStackTrace();
        }

    }


}