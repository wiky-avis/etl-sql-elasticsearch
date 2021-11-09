# etl-sql-elasticsearch
Python-скрипт, который перегружает данные из SQLite в индекс movies в Elasticsearch.

## Установка

Склонируйте репозиторий на локальную машину:

  `git clone https://github.com/wiky-avis/etl-sql-elasticsearch.git`
  
Создайте виртуальное окружение:

  `python -m venv venv`
  
  и активируйте его (команда зависит от ОС:

  `source venv/bin/activate`
  
Ус тановите необходимые зависимости:

  `pip install -r requirements.txt`

Запустите docker-compose:

  `docker-compose up -d`

## Запуск скрипта

  `python sqlite3_es_bulk.py`

После запуска скрипт автоматически создаст индекс movies в Elasticsearch и загрузит в него данные. База данных с фильмами и набор тестов для [Postman](https://www.postman.com/downloads/) в комплекте.
