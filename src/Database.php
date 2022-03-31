<?php

namespace ycl123\queue;

use JsonException;
use think\db\ConnectionInterface;
use think\db\exception\DataNotFoundException;
use think\db\exception\DbException;
use think\db\exception\ModelNotFoundException;
use think\facade\Db;

final class Database
{
    /**
     * @var array 配置
     */
    private $config;

    /**
     * @var ConnectionInterface 数据库连接
     */
    private $dbConnect;

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->dbConnect = $this->getConnect();
    }

    /**
     * 获取数据库连接
     * @return ConnectionInterface
     */
    public function getConnect(): ConnectionInterface
    {
        $connection = $this->config['connection'] ?? null;
        if (is_array($connection)) {
            Db::setConfig(['connections' => ['connect' => $connection]]);
            $connection = 'connect';
        }
        return Db::connect($connection);
    }

    /**
     * 获取cron调度器数据
     * @return array
     * @throws DataNotFoundException
     * @throws DbException
     * @throws ModelNotFoundException
     */
    public function getCronSchedulerList(): array
    {
        return $this->dbConnect->name('task_scheduler')
            ->where('scheduler_status', 1)
            ->where('scheduler_type', 1)
            ->field('id, task_topic, task_data, scheduler_rule AS cron')
            ->select()->toArray();
    }

    /**
     * 获取时间调度器数据
     * @param int $time
     * @return array
     * @throws DataNotFoundException
     * @throws DbException
     * @throws ModelNotFoundException
     */
    public function getTimeSchedulerList(int $time): array
    {
        return $this->dbConnect->name('task_scheduler')
            ->where('scheduler_status', 1)
            ->where('scheduler_type', 2)
            ->where('scheduler_rule', '<=', $time)
            ->field('id, task_topic, task_data, scheduler_rule AS cron')
            ->select()->toArray();
    }

    /**
     * 获取分发器数据
     * @return array
     * @throws DataNotFoundException
     * @throws DbException
     * @throws ModelNotFoundException
     */
    public function getDistributorList(): array
    {
        return $this->dbConnect->name('task_distributor')
            ->where('execute_status', 1)
            ->field('id, task_topic, task_data')
            ->select()->toArray();
    }

    /**
     * 获取队列数据
     * @param int $time
     * @param array $excludeTopic
     * @param int $selectCount
     * @return array
     * @throws DataNotFoundException
     * @throws DbException
     * @throws ModelNotFoundException
     */
    public function getQueueList(int $time, array $excludeTopic, int $selectCount): array
    {
        return $this->dbConnect->name('task_queue')
            ->where('execute_status', 1)
            ->where('next_execute_time', '<=', $time)
            ->when($excludeTopic, function ($query) use ($excludeTopic) {
                $query->whereNotIn('task_topic', $excludeTopic);
            })
            ->order('id', 'asc')
            ->limit($selectCount)
            ->field('id queue_id, task_topic')
            ->select()->toArray();
    }

    /**
     * 获取可重试队列id列表
     * @param array $queueIds
     * @param int $time
     * @param bool $isRetryable
     * @return array
     */
    public function getRetryQueueIds(array $queueIds, int $time, bool $isRetryable): array
    {
        return $this->dbConnect->name('task_queue')
            ->where('execute_status', 2)
            ->whereNotIn('id', $queueIds)
            ->where('start_time', '<', $time - $this->config['task_timeout_expired_time'])
            ->where('execute_count', ($isRetryable ? '<' : '>='), Db::raw('max_execute_count - 1'))
            ->column('id');
    }

    /**
     * 获取队列信息
     * @param $queueId
     * @return array
     * @throws DataNotFoundException
     * @throws DbException
     * @throws ModelNotFoundException
     */
    public function getQueue($queueId): array
    {
        return $this->dbConnect->name('task_queue')
            ->where('id', $queueId)
            ->where('execute_status', 1)
            ->find() ?: [];
    }

    /**
     * 禁用调度器
     * @param int $schedulerId
     * @return int
     * @throws DbException
     */
    public function disableScheduler(int $schedulerId): int
    {
        return $this->dbConnect->name('task_scheduler')
            ->where('id', $schedulerId)
            ->where('scheduler_status', 1)
            ->update(['scheduler_status' => 2]);
    }

    /**
     * 开始执行分发器
     * @param int $distributorId
     * @param int $taskCount
     * @return int
     * @throws DbException
     */
    public function startDistributor(int $distributorId, int $taskCount): int
    {
        return $this->dbConnect->name('task_distributor')
            ->where('id', $distributorId)
            ->where('execute_status', 1)
            ->update(['task_count' => $taskCount, 'execute_status' => 2, 'start_time' => time()]);
    }

    /**
     * 开始执行队列
     * @param int $queueId
     * @param int $maxExecuteCount
     * @return int
     * @throws DbException
     */
    public function startQueue(int $queueId, int $maxExecuteCount): int
    {
        return $this->dbConnect->name('task_queue')
            ->where('id', $queueId)
            ->where('execute_status', 1)
            ->update([
                'execute_status' => 2,
                'execute_result' => '',
                'max_execute_count' => $maxExecuteCount,
                'start_time' => time()
            ]);
    }

    /**
     * 开始队列日志
     * @param int $queueId
     * @return int|string
     */
    public function startQueueLog(int $queueId)
    {
        return $this->dbConnect->name('task_queue_log')
            ->insert(['queue_id' => $queueId, 'execute_status' => 1, 'start_time' => time()]);
    }

    /**
     * 结束执行分发器
     * @param int $distributorId
     * @param int $status
     * @param string $result
     * @return int
     * @throws DbException
     */
    public function endDistributor(int $distributorId, int $status, string $result): int
    {
        return $this->dbConnect->name('task_distributor')
            ->where('id', $distributorId)
            ->where('execute_status', 2)
            ->update(['execute_status' => $status, 'execute_result' => $result, 'end_time' => time()]);
    }

    /**
     * 结束队列
     * @param array $queueIds
     * @param int $time
     * @param bool $isRetryable
     * @return int
     * @throws DbException
     */
    public function endRetryQueues(array $queueIds, int $time, bool $isRetryable): int
    {
        return $this->dbConnect->name('task_queue')
            ->where('execute_status', 2)
            ->whereIn('id', $queueIds)
            ->update([
                'execute_status' => $isRetryable ? 1 : 4,
                'execute_result' => '异常终止，' . ($isRetryable ? '等待重试' : '不可重试'),
                'end_time' => $time,
                'execute_count' => Db::raw('execute_count + 1'),
                'next_execute_time' => $isRetryable ? ($time + $this->config['task_retry_time_interval']) : 0,
            ]);
    }

    /**
     * 结束队列日志
     * @param array $queueIds
     * @param int $time
     * @param bool $isRetryable
     * @return int
     * @throws DbException
     */
    public function endRetryQueueLogs(array $queueIds, int $time, bool $isRetryable): int
    {
        return $this->dbConnect->name('task_queue_log')
            ->where('execute_status', 1)
            ->whereIn('queue_id', $queueIds)
            ->update([
                'execute_status' => 3,
                'execute_result' => '异常终止，' . ($isRetryable ? '等待重试' : '不可重试'),
                'end_time' => $time
            ]);
    }

    /**
     * 结束队列
     * @param int $queueId
     * @param int $time
     * @param int $status
     * @param string $result
     * @return int
     * @throws DbException
     */
    public function endQueue(int $queueId, int $time, int $status, string $result): int
    {
        return $this->dbConnect->name('task_queue')
            ->where('id', $queueId)
            ->where('execute_status', 2)
            ->update([
                'execute_status' => $status,
                'execute_result' => $result,
                'end_time' => $time,
                'execute_count' => Db::raw('execute_count + 1'),
                'next_execute_time' => $status === 1 ? ($time + $this->config['task_retry_time_interval']) : 0,
            ]);
    }

    /**
     * 结束队列日志
     * @param int $queueId
     * @param int $time
     * @param int $status
     * @param string $result
     * @return int
     * @throws DbException
     */
    public function endQueueLog(int $queueId, int $time, int $status, string $result): int
    {
        return $this->dbConnect->name('task_queue_log')
            ->where('queue_id', $queueId)
            ->where('execute_status', 1)
            ->update([
                'execute_status' => $status,
                'execute_result' => $result,
                'end_time' => $time
            ]);
    }

    /**
     * 批量保存分发器数据列表
     * @param array $schedulerList
     * @return int
     */
    public function insertAllDistributorList(array $schedulerList): int
    {
        $distributorList = [];
        $createTime = time();
        foreach ($schedulerList as $scheduler) {
            $distributorList[] = [
                'scheduler_id' => $scheduler['id'],
                'task_topic' => $scheduler['task_topic'],
                'task_data' => $scheduler['task_data'],
                'execute_status' => 1,
                'create_time' => $createTime,
            ];
        }
        if (empty($distributorList)) {
            return 0;
        }
        return $this->dbConnect->name('task_distributor')->insertAll($distributorList, 500);
    }

    /**
     * 批量保存队列数据列表
     * @param int $distributorId
     * @param array $taskList
     * @param string $taskTopic
     * @return int
     * @throws JsonException
     */
    public function insertAllQueueList(int $distributorId, array $taskList, string $taskTopic): int
    {
        $queueList = [];
        $createTime = time();
        foreach ($taskList as $taskData) {
            $queueList[] = [
                'distributor_id' => $distributorId,
                'task_topic' => $taskTopic,
                'task_data' => Start::jsonEncode($taskData),
                'execute_status' => 1,
                'execute_count' => 0,
                'create_time' => $createTime,
            ];
        }
        if (empty($queueList)) {
            return 0;
        }
        return $this->dbConnect->name('task_queue')->insertAll($queueList, 500);
    }
}
