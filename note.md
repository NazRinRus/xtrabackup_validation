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
- def status_cluster(): возвращает True если активен сервис и False если не активен
