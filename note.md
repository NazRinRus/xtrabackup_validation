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
