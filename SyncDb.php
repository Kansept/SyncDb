<?php
class SyncDb
{
    /**
     * Массив с настройками
     *
     * @var array
     */
    private $conf;

    /**
     * Подключение к базе данных для экспорта
     *
     * @var mysqli
     */
    private $db_import;

    /**
     * Массив с настройками
     *
     * @param mysqli $conf
     */
    public function __construct($conf)
    {
        $this->conf = $conf;
        
    }

    /**
     * Выполнить синхронизацию
     *
     * @return void
     */
    public function run()
    {
        $this->clearDir();
        $this->export();
        $this->import();
        $this->clearDir();
    }


    
    /**
     * Экспорт таблиц в csv файлы
     *
     * @return void
     */
    private function export()
    {
        if (is_array($this->conf['db_copy'])) {
            $db_export = new mysqli($this->conf['db_copy']['host'], $this->conf['db_copy']['user'], $this->conf['db_copy']['pass'], $this->conf['db_copy']['db']) or die('not connect to local db');
            $db_export->set_charset($this->conf['db_copy']['charset']);
        } else {
            $db_export = $this->conf['db_copy'];
        }

        foreach ($this->conf['tables'] as $table => $options) {
            echo "Export table {$table}" . PHP_EOL;
            $where = '';
            switch ($options['type']) {
                case 'filter':
                    $where = 'WHERE ' . $options['where'];
                    break;
                default:
                    break;
            }

            $query = $db_export->query("SELECT COUNT(*) `count` FROM {$table} {$where}");
            $info = $query->fetch_assoc();
            $query->close();

            if ($info['count'] > 0) {
                $limit = $this->conf['limit'];
                $file = realpath($this->conf['dir']) . '/' . $table . '.csv';
                $fp = fopen($file, 'w');
                if ($fp === false) {
                    throw new \Exception("Error create file {$file}", 1);
                }
                $iter = ((int) ($info['count'] / $limit) + 1);
                for ($i = 0; $i < $iter; $i++) {
                    $offset = $i * $limit;
                    $query = $db_export->query("SELECT * FROM `{$table}` {$where} LIMIT {$limit} OFFSET {$offset}");
                    while ($r = $query->fetch_assoc()) {
                        $values = array_values($r);
                        foreach ($values as $key => $value) {
                            $values[$key] = $db_export->escape_string($value);
                        }
                        $str = implode('","', $values);
                        fwrite($fp, '"' . $str . '"' . PHP_EOL);
                    }
                    $query->close();
                }
                fclose($fp);
            }
        }

        $db_export->autocommit(true);

        if (is_array($this->conf['db_copy'])) {
            $db_export->close();
        }
    }

    /**
     * Импорт базы данных
     *
     * @return void
     */
    private function import()
    {
        if (is_array($this->conf['db_insert'])) {
            $this->db_import = new mysqli($this->conf['db_insert']['host'], $this->conf['db_insert']['user'], $this->conf['db_insert']['pass'], $this->conf['db_insert']['db']) or die('not connect to remote db');
            $this->db_import->set_charset($this->conf['db_insert']['charset']);
        } else {
            $this->db_import = $this->conf['db_insert'];
        }

        $this->db_import->autocommit(FALSE);

        foreach ($this->conf['tables'] as $table => $options) {
            echo "Import table {$table}" . PHP_EOL;
            if (($handle = fopen($this->conf['dir'] . $table . '.csv', "r")) === false) {
                throw new Exception("Error open file " . $this->conf['dir'] . $table . '.csv', 1);
            }

            if (isset($options['columns'])) {
                $columns = '(' . implode(',', $options['columns']) . ')';
            } else {
                $columns = '';
            }

            switch ($options['type']) {
                case 'full':
                    $this->importFull($handle, $table, $columns);
                    break;
                case 'filter':
                    $this->importFilter($handle, $table, $columns);
                default:
                    break;
            }

            fclose($handle);
        }

        $this->db_import->autocommit(true);

        if (is_array($this->conf['db_insert'])) {
            $this->db_import->close();
        }
    }

    /**
     * Очистить папку от временных файлов
     *
     * @return void
     */
    private function clearDir()
    {
        $dir = realpath($this->conf['dir']);
        if (is_dir($dir)) {
            foreach ($this->conf['tables'] as $table => $options) {
                $file = $dir . '/' . $table . '.csv';
                if (is_file($file)) {
                    unlink($file);
                }
            }
        } else {
            mkdir($this->conf['dir'], 0777, true);
        }
    }

    private function importFull($handle, $table, $columns)
    {
        $this->db_import->query("DELETE FROM `{$table}`");
        $sql = '';
        $row = 0;
        while (($data = fgetcsv($handle, 0, ',', '"')) !== FALSE) {
            $values = implode('","', $data);
            if ($row === 0) {
                $sql = "INSERT INTO `{$table}` {$columns} VALUES (\"{$values}\")";
            } else {
                $sql .= ", (\"{$values}\")";
            }
            if ($row === 1000) {
                if (!$this->db_import->query($sql)) {
                    throw new \Exception($this->db_import->error, 1);
                }
                $sql = '';
            }
            $row = ($row === 1000) ? 0 : $row + 1;
        }
        if (!$this->db_import->query($sql)) {
            throw new \Exception($this->db_import->error, 1);
        }
        $this->db_import->commit();
    }

    private function importFilter($handle, $table, $columns)
    {
        $sql = '';
        $row = 0;
        while (($data = fgetcsv($handle, 0, ',', '"')) !== FALSE) {
            $values = implode('","', $data);
            $this->db_import->query("REPLACE INTO `{$table}` VALUES (\"{$values}\")");
            if ($row === 1000) {
                if (!$this->db_import->query($sql)) {
                    throw new \Exception($this->db_import->error, 1);
                }
                $sql = '';
            }
            $row = ($row === 1000) ? 0 : $row + 1;
        }
        if (!$this->db_import->query($sql)) {
            throw new \Exception($this->db_import->error, 1);
        }
        $this->db_import->commit();
    }
}
