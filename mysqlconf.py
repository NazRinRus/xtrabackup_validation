
MYSQL_DATA_DIR = '/data/mysql'
BACKUP_DIR = '/test_backup'
STATS_DIR = '/var/log/backup_validation'
TRUE_DUMP_DIR = '/test_dump'
TRUE_DUMP = False
CLUSTER_NAMES = ['crm_prod', 'any_test_db']

# Конфигурационные настройки MySQL в скрипте валидации не используются, это задел на будущее
MYSQL_CONFIG_FILE = '/etc/mysql/my.cnf'
MYSQL_CONFIG = {
    'mysqld':{
        'datadir': '/data/mysql',
    },
}

if __name__ == "__main__":
    print('This is a configuration file')
