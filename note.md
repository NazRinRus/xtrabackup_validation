## Заметки по ходу разработки
### Рабочая директория
```
mkdir -p ~/projects/xtrabackup_validation
cd ~/projects/xtrabackup_validation
```
### Виртуальное окружение
Создание окружения:
```
cd ~/venv/
python3 -m venv xtrabackup_validation
```
Активация окружения:
```
source ~/venv/xtrabackup_validation/bin/activate
```
### GIT репозиторий
1. На GitHub создать новый проект, например `xtrabackup_validation`
2. Переходим в локальную директорию проекта `cd ~/projects/xtrabackup_validation` и выполняем:
```
echo "# xtrabackup_validation" >> README.md
git config --global init.defaultBranch main # по умолчанию в локальном git основная ветка master, а в github - main
git init
# если не менял дефолтное название ветки, то можно поменять созданную ветку командой git branch -m main
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin git@github.com:NazRinRus/xtrabackup_validation.git
git push -u origin main
```
### Текущие задачи
1. Убрать тестовые print(), информацию выводить при помощи logging:
```
import logging

def setup_logging(log_level="INFO"):
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
        datefmt='%d/%m/%Y %H:%M:%S',
        level=log_level
    )
```
Куда внедрить логгирование, в класс или в скрипт?
- Если логировать в классе, то класс будет зависить еще от одной внешней библиотеки;
- Если вынести в скрипт, то если метод возвращает True - вывожу положительное сообщение, если False - отрицательное. Куда и как выводить отловленные внутри метода ошибки? Может возвращать результат выполнения метода словарем типа: {'status':True, 'text':'OK'}?

### Модификация класса
2. Написать класс Configuration, парсящий и валидирующий конфигурационный файл, с методом get_configuration()
3. Разработать метод снятия дампов с развернутого бэкапа:
- По переданному параметру, определить вид дампа: полный, схема, БД, таблицы
- Перед выполнением команды проверить статус mysql, развернут ли нужный кластер - соответствуют ли список БД в файлах бэкапа списку БД активного инстанса.
- параметр с целевым путем дампа прописать в конфиге, с возможностью изменения дефолтного значения параметром

### Комментарии к задачам
1. Внутри класса следует валидировать значения, например:
```
if not connection_string:
            raise ValueError("Connection string cannot be empty")
```
в случае ошибки, выполнение метода прерывается, а в самом исполняемом коде отлавливается ошибка:
```
# Исполняемый код - обработка на уровне вызова
try:
    db = DatabaseConnection()
    db.connect("invalid_connection_string")
    result = db.execute_query("SELECT * FROM users")
except ValueError as e:
    print(f"Ошибка валидации: {e}")
```
2. Есть ли смысл в текстовом конфигурационном файле, удобнее держать переменные в файле `.py` и импортировать.
3. 
- def status_cluster(): возвращает True если активен сервис и False если не активен;
- выполнено сравнение активного класстера с кластером текущего экземпляра бэкапа, чтобы узнать он сейчас активен или нет;
- в python разработать метод асинхронного снятия дампа, для ускорения процесса, каждую таблицу в отдельном процессе:
1. Есть список таблиц table_list
2. Цикл: for table in table_list:
3. Формирование команды дампа в цикле: dump_cmd = f"mysqldump --single-transaction --events --triggers db_name {table} > /dev/null"
4. Запустить команду dump_cmd в цикле в асинхронном режиме
5. Информация о выполнении команды не нужна, только результат
6. Дождаться выполнения всех процессов
```
import asyncio
from concurrent.futures import ProcessPoolExecutor
import subprocess
import time

def dump_table_sync(table, db_name):
    """Синхронное создание дампа одной таблицы"""
    dump_cmd = [
        'mysqldump',
        '--single-transaction',
        '--events',
        '--triggers',
        db_name,
        table
    ]
    
    try:
        result = subprocess.run(
            dump_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=300  # 5 минут таймаут на таблицу
        )
        return {'table': table, 'success': result.returncode == 0, 'error': None}
    except subprocess.TimeoutExpired:
        return {'table': table, 'success': False, 'error': 'Timeout'}
    except Exception as e:
        return {'table': table, 'success': False, 'error': str(e)}

async def parallel_dump_tables(table_list, db_name, max_workers=None):
    """Параллельный дамп таблиц с использованием ProcessPoolExecutor"""
    if max_workers is None:
        max_workers = min(len(table_list), 8)  # Максимум 8 процессов
    
    loop = asyncio.get_event_loop()
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Создаем задачи
        tasks = [
            loop.run_in_executor(executor, dump_table_sync, table, db_name)
            for table in table_list
        ]
        
        # Ждем завершения всех задач
        results = await asyncio.gather(*tasks)
    
    return results

# Пример использования
async def main_parallel():
    db_name = "your_database"
    table_list = ["table1", "table2", "table3", "table4", "table5"]
    
    print(f"Начинаем параллельный дамп {len(table_list)} таблиц...")
    start_time = time.time()
    
    results = await parallel_dump_tables(table_list, db_name, max_workers=4)
    
    end_time = time.time()
    
    successful = sum(1 for r in results if r['success'])
    failed = sum(1 for r in results if not r['success'])
    
    print(f"Завершено за {end_time - start_time:.2f} секунд")
    print(f"Успешно: {successful}, Неудачно: {failed}")

# asyncio.run(main_parallel())
```
