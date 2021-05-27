package service;

public class Main
{
    public static void main(String[] args)
    {
        KafkaService kafkaService = new KafkaService();
        SparkRDDService sparkRDD = new SparkRDDService();
        CassandraService cassandraService = new CassandraService();

        kafkaService.connect();
        sparkRDD.connect();
        cassandraService.connect();

        cassandraService.insertAllFlight(sparkRDD.countFlightCountry(kafkaService.readAllData()));
        cassandraService.close();
    }

}
