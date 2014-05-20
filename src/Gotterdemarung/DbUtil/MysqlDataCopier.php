<?php

namespace Gotterdemarung\DbUtil;

/**
 * Class MysqlDataCopier
 *
 * Utility class, that copies data from iterator to target table
 *
 * @package Gotterdemarung\DbUtil
 */
class MysqlDataCopier
{
    /**
     * Iterator with data to copy
     *
     * @var \Iterator
     */
    private $_sourceIterator;

    /**
     * Target
     *
     * @var \PDO
     */
    private $_pdoTarget;

    /**
     * Target database table name
     *
     * @var string
     */
    private $_tableTarget;

    /**
     * Constructor
     *
     * @param \Iterator $source      Data source
     * @param \PDO      $target      Connection to database
     * @param string    $targetTable Database table name
     * @throws \InvalidArgumentException
     */
    public function __construct(\Iterator $source, \PDO $target, $targetTable)
    {
        // Validating
        if (!is_string($targetTable)) {
            throw new \InvalidArgumentException('Target table variable must be a string');
        }
        if (empty($targetTable)) {
            throw new \InvalidArgumentException('Target table variable empty');
        }

        $this->_sourceIterator  = $source;
        $this->_pdoTarget = $target;
        $this->_tableTarget = $targetTable;
    }

    /**
     * Starts copying process
     *
     * @param callable|null $converter Function to be called on each row of data. Current row would be passed to it
     *                                 as argument. Function must return array of data to save
     * @return void
     */
    public function startCopying($converter = null)
    {
        foreach ($this->_sourceIterator as $row) {

            if ($converter !== null && \is_callable($converter)) {
                $row = $converter($row);
            }

            $sql = 'INSERT INTO `' . $this->_tableTarget . '` SET ';
            $keys = array();
            foreach ($row as $key => $value) {
                $keys[] = '`' . $key . '` = ?';
            }

            $stmt = $this->_pdoTarget->prepare($sql . implode(', ', $keys));
            $i = 1;
            foreach ($row as $value) {
                $stmt->bindValue($i++, $value);
            }

            $stmt->execute();
            $stmt->closeCursor();
        }
    }
}