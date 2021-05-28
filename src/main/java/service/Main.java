package service;

import lombok.extern.log4j.Log4j;

@Log4j
public class Main
{
    public static void main(String[] args)
    {
        KafkaService kafkaService = new KafkaService();
        SparkRDDService sparkRDD = new SparkRDDService();
        CassandraService cassandraService = new CassandraService();

        log.info("=====START KAFKA=====");
        kafkaService.connect();
        log.info("=====START SPARK=====");
        sparkRDD.connect();
        log.info("=====START CASSANDRA=====");
        cassandraService.connect();

        log.info("=====START SERVICE=====");
        cassandraService.insertAllFlight(sparkRDD.countFlightCountry(kafkaService.readAllData()));
        cassandraService.close();
    }

}
