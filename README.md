# Вычисляем и добавляем колонку "session_id" к датафрейму
![image](https://github.com/RomanGodun/add_session_id_project/assets/40138357/7e0bf597-c7ed-4f79-93a1-01cbad6d376b)

Пример входного датафрейма:

![image](https://github.com/RomanGodun/add_session_id_project/assets/40138357/ca6482c4-a61f-46fe-9b8b-d7a5fb002599)

Пример результирующего датафрейма:

![image](https://github.com/RomanGodun/add_session_id_project/assets/40138357/d61e878f-28e9-4fa9-9939-58326c822d77)


### Описание:
В этом репозитории реализовано решение задачи представленной выше. Мне хотелось не только написать основной алгоритм с помощью **pandas**, 
но и создать некоторые части полноценного продового проекта: логирование, тесты, конфиги

### Где можно посмотреть код:

- Реализация основного алгоритма - _add_session_id/session_addder.py_ (_SessionAdder.add_session_id_)
- Дополнительные утилиты - _add_session_id/utils.py_
- Логгер-декоратор - _add_session_id/utils.py_
- Инициализация конфигов - _add_session_id/config.py_
- Тесты - _tests/test_session_adder.py_:
</br>

### Первый запуск:

Для работы скрипта требуется **python 3.10** и библиотеки, перечисленные в файле _add_session_id/requirements/requirements.txt_

1. В корневой папке проекта требуется запустить файл install.sh
```shell
bash install.sh
```
В этом файле содержатся команды который можно выполнить по отдельности. Первая устанавливает код репозитория как зависимость для вашего интерпретатора. Вторая устанавливает все необходимые библиотеки
```shell
pip install -e .
pip install -r add_session_id/requirements/requirements.txt
```
2. Подготовив окружение мы можем запустить программу, для этого нам нужен файл _add_session_id/main.py_
```shell
python add_session_id/main.py
```
3. В корневой папке есть файл _config.env_. Через него можно осуществить настройку поведения программы
</br>

### Тесты:
тестовый стенд: _intel i5-11400H 2.7Gh, 16гб RAM, запуск из под WSL_

Запустить тесты можно из коневой папки командой:
```shell
pytest -v --capture=no  tests/test_session_adder.py
```
либо:
```shell
bash test.sh
```
**Описание тестов:**

1. Проверка работы генератора датафрейма
2. Проверка правильности работы алгоритма на данных небольшого размера. Происходит сравнение результирующего датафрейма и заранее подготовленного правильного решения
3. Тест изучающий временную сложность. Происходит генерация и обработка нескольких датасетов размером от 10 млн. до 100 млн. строк с шагом 10 млн. Количество покупателей в датасете пропорционально количеству строк

**Ниже тест на временную сложность повторенный три раза:**

![time_test](https://github.com/RomanGodun/add_session_id_project/assets/40138357/023ae974-f727-4175-82ca-6230dad456f9)
![time_test3](https://github.com/RomanGodun/add_session_id_project/assets/40138357/232fcbc2-9a46-4720-90be-87f883f225a6)
![time_test_wc](https://github.com/RomanGodun/add_session_id_project/assets/40138357/0ac15f4a-ddaa-4128-be93-0e319ff4ab1b)
</br>
</br>
Как видно из графика алгоритм работает за **линейное время**. Примерно на 80-90 млн. записей программа упирается в ограничения оперативной памяти, происходит просадка. 

При этом, алгоритм отрабатывает даже на больших значениях! Тем не менее **будьте осторожны** при запуске программы с большими значениями **N_ROWS** или **N_CUSTOMERS**
</br>
</br>
</br>

**Поля конфига:**
```shell
LOGGING_LEVEL - уровень логов в std.err
LOGGING_DIR_LEVEL - Уровень логов в файл
LOG_DIR - Директория куда будут складываться логи. Если не задавать этот конфиг, то логи будут писаться только в std.err

DF_FROM_FILE - Прочитать df из файла csv или сгенерировать его. True или False
INPUT_FILE_PATH - Путь до файла с исходными данными. Применяется если DF_FROM_FILE=True

Если мы решим генерировать, а не читать датафрейм то нам доступны следующие параметры

N_CUSTOMERS - количество различных покупателей в датасете
N_PRODUCTS- количество различных продуктов
N_ROWS - количество строк в датасете (кол-во просмотров товаров)
START_TIME - минимально возможная временная метка в формате "YYYY-MM-dd HH:mm:ss"
END_TIME - максимально возможная временная метка в формате "YYYY-MM-dd HH:mm:ss"


SAVE_GEN_DF - Сохранить или нет в файл сгенерированный датасет
SAVE_GEN_FILE_PATH - Путь куда сохранится сгенерированный датасет. Применяется только если SAVE_GEN_DF=True

SAVE_RES_DF - Сохранить или нет в файл результирующий датасет (с колонкой session_id)
OUTPUT_FILE_PATH - Путь куда сохранится результирующий датасет. Применяется только если SAVE_RES_DF=True
```
