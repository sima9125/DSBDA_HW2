import random
import time
from kafka import KafkaProducer

dict_airport = {'Аэропорт Вильгельмсхафен':'Германия', 'Аэропорт Гибралтар':'Великобритания', 'Аэропорт Глазго Прествик':'Великобритания', 'Аэропорт Голуэй':'Ирландия', 'Аэропорт Гронинген Элде':'Нидерланды', 'Аэропорт Быдгощ Игнацы Ян Падеревский':'Польша', 'Аэропорт Вагар':'Фарерские острова', 'Аэропорт Вангерооге':'Германия', 'Аэропорт Варшава Модлин':'Польша', 'Аэропорт Варна':'Болгария', 'Аэропорт Векшё Смоланд':'Швеция', 'Аэропорт Венеция Марко Поло':'Италия', 'Аэропорт Вентспилс':'Латвия', 'Аэропорт Веце':'Германия', 'Аэропорт Ираклион Никос Казантзакис':'Греция', 'Аэропорт Осло Гардермуэн':'Норвегия', 'Аэропорт Гданьск Лех Валенса':'Польша', 'Аэропорт Каунас':'Литва', 'Аэропорт Бухарест Генри Коандэ':'Румыния', 'Аэропорт Вильнюс':'Литва', 'Аэропорт Хельсинки Вантаа':'Финляндия', 'Аэропорт Пафос':'Кипр', 'Аэропорт Милан Мальпенса':'Италия', 'Аэропорт Афины Элефтериос Венизелос':'Греция', 'Аэропорт Таллин Леннарт Мэри':'Эстония', 'Аэропорт Москва Шереметьево':'Россия', 'Аэропорт Ларнака':'Кипр', 'Аэропорт Мюнхен':'Германия', 'Аэропорт Рим Леонардо да Винчи - Фьюмичино':'Италия', 'Аэропорт Бургас':'Болгария', 'Аэропорт Москва Внуково':'Россия', 'Аэропорт Вена Швехат':'Австрия', 'Аэропорт Альтенрайн Санкт-Галлен':'Швейцария', 'Аэропорт Аоста Коррадо Гекс':'Италия', 'Аэропорт Альгеро Фертилия':'Италия', 'Аэропорт Амстердам Схипхол':'Нидерланды', 'Аэропорт Анталья':'Турция', 'Аэропорт Брауншвейг-Вольфсбург':'Германия', 'Аэропорт Брешиа Монтикьяри':'Италия', 'Аэропорт Будапешт Ференц Лист':'Венгрия', 'Аэропорт Бургос':'Испания', 'Аэропорт Гранада Федерико Гарсия Лорка':'Испания', 'Аэропорт Белфаст-Сити Джордж Бест':'Великобритания', 'Аэропорт Лондон Гатвик':'Великобритания', 'Аэропорт Мадрид Таррагона':'Испания', 'Аэропорт Ле Туке Опаловый берег':'Франция', 'Аэропорт Эдинбург':'Великобритания', 'Аэропорт Мадрид Барахас':'Испания', 'Аэропорт Клермон-Ферран Овернь':'Франция', 'Аэропорт Белград Никола Тесла':'Сербия', 'Аэропорт Берн Бельп':'Швейцария', 'Аэропорт Закинтос Дионисий Соломос':'Греция', 'Аэропорт Задар':'Хорватия', 'Аэропорт Измир Аднан Мендерес':'Турция', 'Аэропорт Москва Домодедово':'Россия'}

def str_time_prop(start, end, format, prop):
	"""Get a time at a proportion of a range of two formatted times.

	start and end should be strings specifying times formated in the
	given format (strftime-style), giving an interval [start, end].
	prop specifies how a proportion of the interval to be taken after
	start.  The returned time will be in the specified format.
	"""

	stime = time.mktime(time.strptime(start, format))
	etime = time.mktime(time.strptime(end, format))

	ptime = stime + prop * (etime - stime)

	return time.strftime(format, time.localtime(ptime))


def random_date(start, end, prop):
	return str_time_prop(start, end, '%Y-%m-%d %H:%M', prop)

def flight_date():
	start_random = random.random()
	from_date = random_date("2020-1-1 0:0", "2020-2-1 23:59", start_random)
	to_date = random_date("2020-1-1 0:0", "2020-2-1 23:59", start_random+random.randint(1, 5)*0.0001)
	return from_date, to_date



###main###
bootstrap_servers = ['localhost:9092']
topicName = 'stream-spark15'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

arr_airport = list(dict_airport.keys())
flight_number = random.randint(100, 1000)
print('Количество генерируемых данных:')
n = int(input())
for i in range(n):
	flight_letter = random.choice('ABCEFHKLMNPRSTVXY')
	flight_number += random.randint(1, 25)
	from_key = random.choice(arr_airport)
	from_airport = str(from_key)+','+str(dict_airport[from_key])

	to_key = random.choice(arr_airport)
	to_airport = str(to_key)+','+str(dict_airport[to_key])
	d = flight_date()

	flight_str = flight_letter+str(flight_number)+','+d[0]+','+d[1]+','+from_airport+','+to_airport
	producer.send(topicName, bytes(flight_str, 'utf-8'))
	producer.flush()

producer.close()
