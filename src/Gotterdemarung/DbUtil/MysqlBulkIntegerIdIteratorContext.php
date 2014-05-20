<?php

namespace Gotterdemarung\DbUtil;

/**
 * Class MysqlBulkIntegerIdIteratorContext
 *
 * Context implementation for cases, when needed to iterate over integer id field in database
 *
 * @package Gotterdemarung\DbUtil
 */
class MysqlBulkIntegerIdIteratorContext implements MysqlBulkIteratorContext
{
    /**
     * ID field name
     *
     * @var string
     */
    private $_idName;

    /**
     * Database collection name
     *
     * @var string
     */
    private $_tableName;

    /**
     * Initial (lowest) ID value. Zero by default
     *
     * @var int
     */
    private $_initial;

    /**
     * Last ID value, read from database
     *
     * @var int
     */
    private $_last;

    /**
     * Constructor
     *
     * @param string $table     Database collection name
     * @param string $idField   Id field name (uses 'id' as default)
     * @param int $initialValue Initial value (uses zero as default)
     *
     * @throws \InvalidArgumentException On invalid incoming arguments
     */
    public function __construct($table, $idField = 'id', $initialValue = 0)
    {
        if (!is_string($table)) {
            throw new \InvalidArgumentException('Table variable must be a string');
        }
        if (!is_string($idField)) {
            throw new \InvalidArgumentException('Id field variable must be a string');
        }
        if (empty($table)) {
            throw new \InvalidArgumentException('Table variable empty');
        }
        if (empty($idField)) {
            throw new \InvalidArgumentException('Id field variable empty');
        }
        if (!is_int($initialValue)) {
            throw new \InvalidArgumentException('Initial value should be an integer');
        }

        $this->_idName    = $idField;
        $this->_tableName = $table;
        $this->_initial   = $initialValue;

        $this->reset();
    }

    /**
     * Points context to begin
     *
     * @return void
     */
    public function reset()
    {
        $this->_last = $this->_initial;
    }

    /**
     * Returns SQL for next bulk data
     *
     * @param int $limit
     * @return string
     * @throws \InvalidArgumentException
     */
    public function getSql($limit)
    {
        if (!is_int($limit) || $limit < 1) {
            throw new \InvalidArgumentException(
                'Invalid limit provided ' . $limit
            );
        }

        return 'SELECT * FROM `' . $this->_tableName . '`'
        . ' WHERE `' . $this->_idName .'` > ' . $this->_last
        . ' ORDER BY `' . $this->_idName .'` ASC'
        . ' LIMIT ' . $limit;
    }

    /**
     * Updates context with information about last data read
     *
     * @param array $data
     * @return void
     * @throws \InvalidArgumentException
     */
    public function updateLastRead(array $data)
    {
        if (!is_array($data) || empty($data) || !isset($data[$this->_idName])) {
            throw new \InvalidArgumentException(
                "Provided data does not contain mandatory {$this->_idName} field"
            );
        }

        $id = $data[$this->_idName];
        if (!is_int($id) && !ctype_digit($id)) {
            throw new \InvalidArgumentException(
                'Id must be a string but received ' . $id
            );
        }

        $this->_last = $id;
    }
}