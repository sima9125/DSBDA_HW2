package service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.util.Map;
import java.util.UUID;

public class CassandraService
{
    private Cluster cluster;
    private Session session;
    private final int port = 9042;

    /**
     * Метод инициализации необходимых параметров
     * Создание пространства java_api, если отсутсвтвует
     * Создание заново таблицы flight
     */
    public void connect()
    {
        Cluster.Builder b = Cluster.builder().addContactPoint("localhost");
        b.withPort(port);
        cluster = b.build();
        session = cluster.connect();

        session.execute("DROP KEYSPACE IF EXISTS java_api");
        session.execute("CREATE KEYSPACE java_api WITH replication= {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE java_api.flight (id UUID PRIMARY KEY,flight_time TEXT, country_from TEXT, country_to TEXT, flight_count BIGINT)");
    }

    public Session getSession()
    {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    /**
     * Запись одного значения в таблицу flight
     */
    private void insertFlight(String flightTime, String countryFrom, String countryTo, long flightCount)
    {
        session.execute("INSERT INTO java_api.flight (id, flight_time, country_from, country_to, flight_count) VALUES (?, ?, ?, ?, ?)",
                UUID.randomUUID(), flightTime, countryFrom, countryTo, flightCount);
    }

    /**
     * обработка всех значений полученных SparkRDD
     */
    public void insertAllFlight(Map<String, Long> map)
    {
        for (Map.Entry entry: map.entrySet())
        {
            String[] strings = entry.getKey().toString().split(",");
            insertFlight(strings[0], strings[1], strings[2], (Long)entry.getValue());
        }
    }
}
