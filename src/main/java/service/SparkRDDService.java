package service;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;

public class SparkRDDService
{
    private SparkContext sc;

    public SparkRDDService()
    {

    }

    /**
     * Метод инициализации необходимых параметров
     */
    public void connect()
    {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Flight")
                .setMaster("local");
        sc = new SparkContext(sparkConf);
        //sc = SparkContext.getOrCreate(sparkConf);
    }

    /**
     * считываем данные из kafka
     * @return возвращаем словарь
     * ключь является связка вида: Город вылета,Город прилета,Дата вылета
     * значение количество таких данных
     */
    public Map<String, Long> countFlightCountry(List<String> data)
    {
        JavaRDD<String> stringRDD = JavaSparkContext.fromSparkContext(sc).parallelize(data).
                map(record ->
                {
                    String[] strings = record.split(",");
                    return String.join(",", strings[1].substring(0, 14)+"00:00",strings[4],strings[6]);
                });

        return stringRDD.countByValue();
    }
}
