import org.junit.Before;
import org.junit.Test;

import service.SparkRDDService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SparkRDDTest
{
    SparkRDDService sparkRDDService;

    @Before
    public void setUp()
    {
        sparkRDDService = new SparkRDDService();
        sparkRDDService.connect();
    }

    @Test
    public void testCountFlightEquallyAirport()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("A0001,2020-01-23 15:14:12,2020-01-23 16:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Голуэй,Ирландия");
        strings.add("A0001,2020-01-23 15:24:12,2020-01-23 16:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Голуэй,Ирландия");
        strings.add("A0001,2020-01-23 16:24:12,2020-01-23 17:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Голуэй,Ирландия");
        strings.add("A0001,2020-02-23 15:24:12,2020-02-23 16:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Голуэй,Ирландия");

        Map<String, Long> map = sparkRDDService.countFlightCountry(strings);
        Map<String, Long> map2 = new HashMap<>();
        map2.put("2020-01-23 15:00:00,Великобритания,Ирландия", Long.valueOf(2));
        map2.put("2020-01-23 16:00:00,Великобритания,Ирландия", Long.valueOf(1));
        map2.put("2020-02-23 15:00:00,Великобритания,Ирландия", Long.valueOf(1));

        assertEquals(map, map2);
    }

    @Test
    public void testCountFlightEquallyCountry()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("A0001,2020-01-23 15:14:12,2020-01-23 16:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Голуэй,Ирландия");
        strings.add("A0001,2020-01-23 15:17:12,2020-01-23 16:14:12,Аэропорт Глазго Прествик,Великобритания,Аэропорт Голуэй,Ирландия");
        strings.add("A0001,2020-01-23 15:47:12,2020-01-23 16:54:12,Аэропорт Глазго Прествик,Великобритания,Аэропорт Дублин,Ирландия");
        strings.add("A0001,2020-01-23 15:18:12,2020-01-23 16:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Дублин,Ирландия");
        strings.add("A0001,2020-01-23 16:18:12,2020-01-23 17:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Дублин,Ирландия");
        strings.add("A0001,2020-02-23 15:18:12,2020-02-23 16:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Дублин,Ирландия");

        Map<String, Long> map = sparkRDDService.countFlightCountry(strings);
        Map<String, Long> map2 = new HashMap<>();
        map2.put("2020-01-23 15:00:00,Великобритания,Ирландия", Long.valueOf(4));
        map2.put("2020-01-23 16:00:00,Великобритания,Ирландия", Long.valueOf(1));
        map2.put("2020-02-23 15:00:00,Великобритания,Ирландия", Long.valueOf(1));

        assertEquals(map, map2);
    }

    @Test
    public void testCountFlight()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("A0001,2020-01-23 15:14:12,2020-01-23 16:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Голуэй,Ирландия");
        strings.add("A0001,2020-01-23 15:17:12,2020-01-23 18:14:12,Аэропорт Гибралтар,Великобритания,Аэропорт Задар,Хорватия");
        strings.add("A0001,2020-01-23 15:17:12,2020-01-23 18:14:12,Аэропорт Задар,Хорватия,Аэропорт Гибралтар,Великобритания");
        strings.add("A0001,2020-01-23 15:56:12,2020-01-23 16:58:12,Аэропорт Будапешт Ференц Лист,Венгрия,Аэропорт Берн Бельп,Швейцария");

        Map<String, Long> map = sparkRDDService.countFlightCountry(strings);
        Map<String, Long> map2 = new HashMap<>();
        map2.put("2020-01-23 15:00:00,Великобритания,Ирландия", Long.valueOf(1));
        map2.put("2020-01-23 15:00:00,Великобритания,Хорватия", Long.valueOf(1));
        map2.put("2020-01-23 15:00:00,Хорватия,Великобритания", Long.valueOf(1));
        map2.put("2020-01-23 15:00:00,Венгрия,Швейцария", Long.valueOf(1));

        assertEquals(map, map2);
    }
}
