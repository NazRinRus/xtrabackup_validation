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
1. Внедрить асинхронное выполнение снятия дампов таблиц:
```
        for db, tables in dbs_tables.items():
            parametrs.append(db)
            for table in tables:
                parametrs.append(f"--tables {table}")
                if cluster_instance.start_dump(param_list=parametrs):
                    logging.info(f"Taking dump table - '{table}' DB - '{db}' from cluster '{cluster_name}' completed successfully")
                parametrs.pop() # удаляю параметр с таблицей
            parametrs.pop() # удаляю параметр с базой
```
Попробовать такой вариант:
```
import multiprocessing
import subprocess
import os

def take_dump(target_host):
    """Функция для снятия дампа для конкретного хоста."""
    try:
        # Пример: использование tcpdump для захвата трафика
        # Замените эту команду на вашу реальную команду снятия дампа
        command = ["tcpdump", "-i", "eth0", f"host {target_host}", "-w", f"dump_{target_host}.pcap"]
        # Запускаем процесс, который будет работать, пока не прервем его
        process = subprocess.Popen(command)
        process.wait() # Ждем завершения процесса, если нужно
        print(f"Снятие дампа с {target_host} завершено в dump_{target_host}.pcap")
    except Exception as e:
        print(f"Ошибка при снятии дампа с {target_host}: {e}")

if __name__ == "__main__":
    # Список целей для снятия дампа
    targets = ["192.168.1.100", "192.168.1.101", "192.168.1.102"]

    # Создаем пул процессов с максимальным количеством доступных ядер
    # pool = multiprocessing.Pool(processes=multiprocessing.cpu_count()) # Использовать все ядра
    pool = multiprocessing.Pool(processes=len(targets)) # Использовать столько процессов, сколько целей

    # Распределяем задачи по процессам
    pool.map(take_dump, targets)

    # Закрываем пул и дожидаемся завершения всех процессов
    pool.close()
    pool.join()

    print("Все процессы снятия дампа завершены.")
```
позволяет разделить таблицы на пулы, например по количеству ядер. Процессы будут дампить свои таблицы параллельно.
Для параллельного снятия дампа в Python следует использовать модуль multiprocessing, так как из-за глобальной блокировки интерпретатора (GIL) потоки (threading) не обеспечивают истинный параллелизм на многоядерных процессорах. Создание нескольких процессов с помощью multiprocessing позволяет каждому процессу иметь свой собственный интерпретатор Python и использовать несколько ядер процессора для параллельного выполнения задач


**Не забывай про адресацию в памяти, работая со списками. Снятие дампа поэтому работает как снежный ком**

#### Тест снятия дампа по отдельным таблицам, одним потоком и процессом:
```
2025-09-24 08:27:02,361 INFO Время снятия дампа с архивированием: 00:01:51
```
