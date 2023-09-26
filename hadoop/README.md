# Практика MapReduce

## Подготовка

* создадим кластер 
* установим AWS CLI на одной из машин кластера:
  ```shell
  apt install -y awscli
  ```
* скачаем тестовые данные:
  ```shell
  aws --profile=karpov-user --endpoint-url=https://storage.yandexcloud.net
  s3 cp --recursive s3://ny-taxi-data/ny-taxi/ ./2020
  ```
* переложим их на HDFS:
  ```shell
  hadoop fs -mkdir /user/root/2020
  hadoop fs -put /root/2020 2020
  ```  
## Разберем исходный код mapper и reducer
## Запустим код
* Скопируем код на кластер
  ```shell
  scp ./*.py root@<Публичный IP>:/tmp/mapreduce/
  scp ./run.sh root@<Публичный IP>:/tmp/mapreduce/
  ```
* Запустим код
  ```shell
  cd /tmp/mapreduce/
  chmod +x ./run.sh
  ./run.sh 
  ```
