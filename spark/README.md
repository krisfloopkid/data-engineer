### Обзор

На практике использовался набор данных [Задержки и отмены рейсов за 2015 год](https://www.kaggle.com/datasets/usdot/flight-delays).
 Работа велась на подготовленных _parquet_ файлах с данными об _airlines_, _airports_ и _flights_.

---

### Задания

1. Построить сводную таблицу отображающую топ 10 рейсов по коду рейса (`TAIL_NUMBER`) и числу вылетов за все время. Отсечь значения без указания кода рейса.

2. Найти топ 10 авиамаршрутов (`ORIGIN_AIRPORT`, `DESTINATION_AIRPORT`) по наибольшему числу рейсов, а так же посчитать среднее время в полете (`AIR_TIME`).

3. Определить список аэропортов у которых самые больше проблемы с задержкой на вылет рейса.
   Вычислить среднее, минимальное, максимальное время задержки и выбрать аэропорты только те где максимальная задержка (`DEPARTURE_DELAY`) _1000_ секунд и больше.
   Дополнительно посчитать корреляцию между временем задержки и днем недели (`DAY_OF_WEEK`)


4. Собрать таблицу на основе данных с требуемыми полями:
`AIRLINE_NAME`, `TAIL_NUMBER`, `ORIGIN_COUNTRY`, `ORIGIN_AIRPORT_NAME`, `ORIGIN_LATITUDE`, `ORIGIN_LONGITUDE`, 
`DESTINATION_COUNTRY`, `DESTINATION_AIRPORT_NAME`, `DESTINATION_LATITUDE`, `DESTINATION_LONGITUDE`.


5. Построить сводную таблицу о всех авиакомпаниях содержащую следующие данные:
   - `AIRLINE_NAME` - название авиалинии [airlines.AIRLINE]
   - `correct_count` - число выполненных рейсов
   - `diverted_count` - число рейсов выполненных с задержкой
   - `cancelled_count` - число отмененных рейсов
   - `avg_distance` - средняя дистанция рейсов
   - `avg_air_time`- среднее время в небе
   - `airline_issue_count` - число задержек из-за проблем с самолетом (`CANCELLATION_REASON`)
   - `weather_issue_count` - число задержек из-за погодных условий (`CANCELLATION_REASON`)
   - `nas_issue_count` - число задержек из-за проблем (`CANCELLATION_REASON`)
   - `security_issue_count` - число задержек из-за службы безопасности (`CANCELLATION_REASON`)

