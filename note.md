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
1. Перед написанием класса, протестировать функции
2. Написать метод остановки и запуска кластера
3. Переписать функции на методы
4. Протестировать атрибуты и методы класса:
```
>>> crm_prod = models.MySQL_cluster('crm_prod')
>>> crm_prod.cluster_name
'crm_prod'
>>> crm_prod.backupdir
'/test_backup'
>>> crm_prod.username
'mysql'
>>> crm_prod.mysql_data_dir
'/data/mysql'
>>> crm_prod.stop_cluster()
Service mysql stop successful
True
>>> crm_prod.start_cluster()
Service mysql start successful
True
>>> crm_prod.clear_data_dir()
Successfully cleared /data/mysql as mysql
True
>>> crm_prod.copy_backup_in_datadir()
Copy completed successfully
True
>>> crm_prod.xtrabackup_restore()
Restore completed successfully
True
>>> crm_prod.start_cluster()
Service mysql start successful
True
```
Протестировать следующее:

??? stop_cluster и start_cluster не получают атрибуты экземпляров, всегда одинаковы, наверное их можно сделать статическими методами, попробуй...

!!! Да, методы общие для всего класса, не использующие переменные экземпляра, можно не передавать self, использовать декоратор @staticmethod

??? Переменные mysql_data_dir и username объявлены как переменные класа MySQL_cluster, как их вызвать в методе?

!!! Методы, например `def clear_data_dir(self)`, использует только переменные класса, которые можно вызвать следующим образом: `MySQL_cluster.mysql_data_dir`, `MySQL_cluster.username`. Возможно ли сделать этот метод статическим?

Исправил метод на статический, все работает:
```
@staticmethod
def clear_data_dir():
...
>>> crm_prod.clear_data_dir()
Successfully cleared /data/mysql as mysql
True
```
**Атрибуты и методы класса внедрены и протестированы, отдельные функции вне класса можно удалять**
### Модификация класса
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

**Принятое решение**
Внутри класса следует валидировать значения, например:
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
2. Переписать класс с валидацией ошибок
Класс переписан, требуется протестировать, часть тестов можно на локальной машине
3. Написать метод получения списка БД в кластере. Проверить есть ли возможность достать инфу о БД из файлов бэкапа, т.к. для для этого не обязательно чтобы кластер был уже развернут. На крайняк можно SQL-запросом.
4. Переписать методы со staticmethod на classmethod
5. Метод вычленения и подстановки значений в файл можно сделать приватным, он не запускается из вне.
