<?php

namespace Gotterdemarung\DbUtil;

/**
 * Interface MysqlBulkIteratorContext
 *
 * Describes context, used inside database iterator
 * @package Gotterdemarung\DbUtil
 */
interface MysqlBulkIteratorContext
{
    /**
     * Points context to begin
     *
     * @return void
     */
    public function reset();

    /**
     * Returns SQL for next bulk data
     *
     * @param int $limit
     * @return string
     * @throws \InvalidArgumentException
     */
    public function getSql($limit);

    /**
     * Updates context with information about last data read
     *
     * @param array $data
     * @return void
     * @throws \InvalidArgumentException
     */
    public function updateLastRead(array $data);
}