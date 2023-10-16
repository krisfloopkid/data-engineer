### Обзор

На практике использовался набор данных [Нью-Йоркского такси за 2020 год](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
> [Описание формата данных](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

---

### Подготовка

- Создать S3 бакет в Object Storage (документация)
- Скопировать данные желтого такси за 2020 год в созданный s3 бакет.


### Задача

Написать map-reduce приложение, использующее скопированные на Object storage данные и вычисляющее отчет на каждый месяц 2020 года вида:

| Payment type | Month   | Tips average amount |
|--------------|---------|---------------------|
| Cash         | 2020-01 | 999.99              |

### Требования к отчету

- Количество файлов — 1
- Формат — csv
- Сортировка — не требуется
- Month считаем по полю tpep_pickup_datetime
- Payment type считаем по полю payment_type
- Tips average amount вычисляем по полю tip_amount
- В датасете будут присутствовать неверные данные, которые нужно будет отсеять
- Маппинг типов оплат:
```python
mapping = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }
```
