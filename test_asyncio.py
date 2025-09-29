#!/usr/bin/env python3

import os
import multiprocessing
import logging
import subprocess
import time
from models import MySQL_cluster, format_time, archive_file, tasks_building
from mysqlconf import CLUSTER_NAMES, TRUE_DUMP_DIR
import asyncio

logging.basicConfig(level=logging.INFO, filename="x_validation.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

async def producer_async(task_queue, task):
    await task_queue.put(task)


async def start_dump(task_queue): 
    while True:
        command_str = await task_queue.get()
        process = await asyncio.create_subprocess_shell(
            f"sudo {command_str}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
            )
        stdout, stderr = await process.communicate()
        task_queue.task_done()

async def main_queue():
    dbs_tables = cluster_instance.get_tables_in_dbs()
    # базовые параметры для снятия дампа только данных одной таблицы
    parametrs = [
            "--no-create-info",
            "--single-transaction",
            "--set-gtid-purged=OFF",
            "--skip-triggers",
            "--compact", 
            "--complete-insert"
        ]
    tasks = tasks_building(dbs_tables, parametrs)
    task_queue = asyncio.Queue(maxsize=20)

    optimal_workers = 20  # Не более 20 потоков
    producers = [asyncio.create_task(producer_async(task_queue, task)) for task in tasks]
    consumers = [asyncio.create_task(start_dump(task_queue)) for _ in range(optimal_workers)]

    # Ждем, пока продюсеры добавят все элементы
    await asyncio.gather(*producers)
    print("-- Продюсеры завершили --")

    await task_queue.join()
    print("-- Все элементы обработаны --")

    # Аккуратно останавливаем потребителей (т.к. они в бесконечном цикле)
    for c in consumers:
        c.cancel()

    # Даем возможность задачам обработать отмену
    await asyncio.gather(*consumers, return_exceptions=True)
    print("-- Потребители остановлены --")

if __name__ == "__main__":
    cluster_name = 'crm_prod'
    cluster_instance = MySQL_cluster(cluster_name)

    val_start_time = time.time()
    exit_code = 0
    try:
        asyncio.run(main_queue())
    except RuntimeError as e:
        if "cannot run current event loop" in str(e):
            print("Запуск через asyncio.run() не удался. Попробуйте другой способ запуска цикла событий.")
        else:
            raise e
            exit_code = 1
            logging.error(e)
    
    # Блок формирования отчета о текущей итерации
    val_stop_time = time.time()
    val_duration = val_stop_time - val_start_time
    cluster_instance.val_durations[cluster_name] = format_time(val_duration)
    logging.info(f"Время снятия дампа: {format_time(val_duration)}")
    cluster_instance.exit_codes[cluster_name] = exit_code
    cluster_instance.sizes[cluster_name] = cluster_instance.get_size_cluster()
