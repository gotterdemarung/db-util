<?php

namespace Gotterdemarung\DbUtil;

/**
 * Class MysqlBulkIterator
 *
 * @package Gotterdemarung\DbUtil
 */
class MysqlBulkIterator implements \Iterator
{
    /**
     * @var \PDO Data container
     */
    private $_pdo;
    /**
     * @var int Bulk size
     */
    private $_bulkSize;
    /**
     * @var MysqlBulkIteratorContext Context, handling next data part restrictions
     */
    private $_ctx;

    /**
     * Current overall iteration index
     *
     * @var int
     */
    private $_index       = 0;
    /**
     * Current buffer read index
     *
     * @var int
     */
    private $_bufferIndex = 0;
    /**
     * Current data buffer
     *
     * @var array
     */
    private $_buffer      = null;
    /**
     * If true, navigation allowed
     *
     * @var bool
     */
    private $_finished    = false;

    /**
     * Constructor
     *
     * @param \PDO                     $pdo      PDO data source
     * @param int                      $bulkSize Amount of data to retrieve per SQL request
     * @param MysqlBulkIteratorContext $context  Iteration context
     * @throws \InvalidArgumentException
     */
    public function __construct(\PDO $pdo, $bulkSize, MysqlBulkIteratorContext $context)
    {
        if (!is_int($bulkSize)) {
            throw new \InvalidArgumentException('Bulk size must be integer');
        }
        if ($bulkSize < 1 || $bulkSize > 1000000) {
            throw new \InvalidArgumentException(
                'Invalid bulk size provided, received ' . $bulkSize
            );
        }

        $this->_pdo      = $pdo;
        $this->_bulkSize = $bulkSize;
        $this->_ctx      = $context;
    }

    /**
     * Rebuilds internal buffer
     *
     * @return void
     */
    private function _rebuildBuffer()
    {
        // Reading data
        $stmt = $this->_pdo->prepare($this->_ctx->getSql($this->_bulkSize));

        $stmt->execute();
        $list = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();

        $this->_buffer      = array();
        $this->_bufferIndex = 0;

        if (empty($list)) {
            // All data has been read or table is empty
            $this->_finished = true;
        } else {
            $this->_buffer = $list;
        }
    }

    /**
     * Return the current element
     *
     * @throws \BadMethodCallException
     * @return string[]
     */
    public function current()
    {
        if ($this->_finished) {
            throw new \BadMethodCallException(
                'Call to current without calling valid'
            );
        }
        return $this->_buffer[$this->_bufferIndex];
    }

    /**
     * Move forward to next element
     *
     * @throws \RangeException
     * @return void
     */
    public function next()
    {
        if ($this->_finished) {
            throw new \RangeException(
                'Read after end of data'
            );
        }
        $this->_index++;
        $this->_bufferIndex++;
        if ($this->_bufferIndex === count($this->_buffer)) {
            // Refreshing buffer
            $this->_rebuildBuffer();
        } else {
            $this->_ctx->updateLastRead($this->current());
        }
    }

    /**
     * Return the key of the current element
     *
     * @return mixed scalar on success, or null on failure.
     */
    public function key()
    {
        return $this->_index;
    }

    /**
     * Checks if current position is valid
     *
     * @return boolean
     */
    public function valid()
    {
        return !$this->_finished;
    }

    /**
     * Rewind the Iterator to the first element
     *
     * @return void
     */
    public function rewind()
    {
        $this->_index       = 0;
        $this->_bufferIndex = 0;
        $this->_buffer      = array();
        $this->_finished    = false;
        $this->_rebuildBuffer();
    }
}