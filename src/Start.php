<?php

namespace ycl123\queue;

use Exception;
use JsonException;
use think\Container;
use think\db\ConnectionInterface;
use think\facade\Db;
use Workerman\Connection\TcpConnection;
use Workerman\Worker;

final class Start
{
    /**
     * @var array 配置
     */
    private $config;

    /**
     * @var int 进程id
     */
    private $workerId;

    /**
     * @var int 队列数量
     */
    private $queueCount = 0;

    /**
     * @var int[] 任务主题数量池
     */
    private $taskTopicCountPool = [];

    /**
     * @var int[] 任务队列id池
     */
    private $taskQueueIdPool = [];

    /**
     * @var Task[] 任务主题实例池
     * ['task_topic' => $taskTopicInstance]
     */
    private $taskTopicInstancePool = [];

    /**
     * @var Database
     */
    private $database;

    /**
     * @var string|callable
     */
    private static $log;

    public function __construct(array $config)
    {
        $this->config = $this->getConfig($config);
        self::$log = $this->config['log'] ?: null;
    }

    public function run(): void
    {
        // 实例化worker
        $worker = new Worker('text://' . $this->config['socket'], $this->config['context'] ?? []);

        // 设置worker属性
        $worker->count = $this->config['count'];
        $worker->name = $this->config['name'];
        $worker::$stdoutFile = $this->config['stdoutFile'];
        $worker::$pidFile = $this->config['pidFile'];
        $worker::$logFile = $this->config['logFile'];
        $worker->reloadable = $this->config['reloadable'];
        $worker::$daemonize = $this->config['daemonize'];
        // 设置worker回调
        $worker->onWorkerStart = [$this, 'onWorkerStart'];
        $worker->onConnect = [$this, 'onConnect'];
        $worker->onMessage = [$this, 'onMessage'];

        // 运行worker
        Worker::runAll();
    }

    /**
     * 获取配置
     * @param $config
     * @return array
     */
    private function getConfig($config): array
    {
        $connection = $config['connection'] ?? [];
        return [
            'context' => (array)($config['context'] ?? []),
            'con_content' => (array)($config['con_content'] ?? []),
            'socket' => (string)($config['socket'] ?? '127.0.0.1:9889'),
            'count' => (int)($config['count'] ?? 1),
            'task_concurrent' => (int)($config['task_concurrent'] ?? 50),
            'task_topic_concurrent' => (int)($config['task_topic_concurrent'] ?? 10),
            'task_retry_time_interval' => (int)($config['task_retry_time_interval'] ?? 10),
            'task_timeout_expired_time' => (int)($config['task_timeout_expired_time'] ?? 60),
            'name' => (string)($config['name'] ?? 'Ycl123 queue service'),
            'stdoutFile' => (string)($config['stdoutFile'] ?? '/dev/null'),
            'pidFile' => (string)($config['pidFile'] ?? __DIR__ . '/../worker_ycl123_queue.pid'),
            'logFile' => (string)($config['logFile'] ?? __DIR__ . '/../worker_ycl123_queue.log'),
            'reloadable' => (bool)($config['reloadable'] ?? true),
            'daemonize' => (bool)($config['daemonize'] ?? false),
            'connection' => is_array($connection) ? $connection : (string)$connection,
            'cron_scheduler_time_interval' => (int)((bool)($config['is_cron_scheduler'] ?? false)),
            'time_scheduler_time_interval' => (float)($config['time_scheduler_time_interval'] ?? 1),
            'distributor_time_interval' => (float)($config['distributor_time_interval'] ?? 1),
            'queue_time_interval' => (float)($config['queue_time_interval'] ?? 1),
            'overdue_time_interval' => (float)($config['overdue_time_interval'] ?? 30),
            'log' => is_callable($config['log'] ?? null) ? $config['log'] : null,
            'task_dirs' => (array)($config['task_dirs'] ?? []),
            'task_class' => (array)($config['task_class'] ?? []),
        ];
    }

    /**
     * 记录日志
     * @param Exception|string $msg
     */
    public static function recordLog($msg): void
    {
        if (is_string(self::$log)) {
            (self::$log)($msg);
        } else {
            Worker::log($msg);
        }
    }

    /**
     * 对变量进行 JSON 编码
     * @param $value
     * @return string
     * @throws JsonException
     */
    public static function jsonEncode($value): string
    {
        return json_encode($value ?: [], JSON_THROW_ON_ERROR) ?: '';
    }

    /**
     * 对 JSON 格式的字符串进行解码
     * @param $json
     * @return array
     * @throws JsonException
     */
    public static function jsonDecode($json): array
    {
        return json_decode($json ?: '[]', true, 512, JSON_THROW_ON_ERROR) ?: [];
    }

    /**
     * 监听成功
     * @param Worker $worker
     * @throws Exception
     */
    public function onWorkerStart(Worker $worker): void
    {
        $this->workerId = $worker->id;
        if ($worker->id === 0) {
            // 因主进程需要进行调度等，所以给主进程五个任务的空闲去进行调度等
            $this->queueCount = 5;
            $mainProcess = new MainProcess($this->config);
            $mainProcess->run();
        }
        $this->database = new Database($this->config);
    }

    /**
     * 连接建立
     * @param TcpConnection $connection
     */
    public function onConnect(TcpConnection $connection): void
    {
        $this->sendMessage($connection, 'connect', [
            'queue_count' => $this->queueCount,
            'task_topic_pool_count' => $this->taskTopicCountPool,
            'task_queue_id_pool' => $this->taskQueueIdPool,
        ]);
    }

    /**
     * worker收到数据时触发
     * @param TcpConnection $connection
     * @param $receiveData
     */
    public function onMessage(TcpConnection $connection, $receiveData): void
    {
        ++$this->queueCount;
        try {
            if (empty($receiveData = self::jsonDecode($receiveData))) {
                return;
            }
            $type = (string)$receiveData['type'];
            $time = (int)$receiveData['time'];
            $data = (array)$receiveData['data'];
            switch ($type) {
                // cron调度
                case 'cron_scheduler':
                    $this->cronScheduler($time);
                    break;
                // time调度
                case 'time_scheduler':
                    $this->timeScheduler($time);
                    break;
                // 任务分发
                case 'task_distributor':
                    $this->taskDistributor();
                    break;
                // 队列调度
                case 'queue_scheduler':
                    $this->queueScheduler($connection, $data, $time);
                    break;
                // 任务执行
                case 'execute_task':
                    // 执行任务
                    $this->executeTask($data);
                    // 执行任务完成
                    $this->sendMessage($connection, 'execute_task_finished', $data);
                    break;
                // 处理逾期任务
                case 'overdue_tasks':
                    $this->handleOverdueTasks($data, $time);
                    break;
            }
            // 任务处理完成
            $this->sendMessage($connection, 'finished');
        } catch (Exception $exception) {
            self::recordLog($exception);
        }
        --$this->queueCount;
    }

    /**
     * 发送消息
     * @param TcpConnection $connection
     * @param string $type
     * @param array $data
     */
    private function sendMessage(TcpConnection $connection, string $type, array $data = []): void
    {
        try {
            $sendData = ['type' => $type, 'worker_id' => $this->workerId, 'time' => time(), 'data' => $data];
            $connection->send(self::jsonEncode($sendData));
        } catch (Exception $exception) {
            self::recordLog($exception);
        }
    }

    /**
     * cron调度器
     * @param int $time
     */
    private function cronScheduler(int $time): void
    {
        try {
            // 获取调度器数据
            $schedulerList = $this->database->getCronSchedulerList();
            // 获取可执行的调度器数据
            foreach ($schedulerList as $key => $scheduler) {
                if (!Cron::isExecuteCronTime($scheduler['cron'], $time)) {
                    unset($schedulerList[$key]);
                }
            }
            // 将数据插入到分发器表中
            $this->database->insertAllDistributorList($schedulerList);
        } catch (Exception $exception) {
            self::recordLog($exception);
        }
    }

    /**
     * 时间调度器
     * @param int $time
     */
    private function timeScheduler(int $time): void
    {
        try {
            // 获取调度器数据
            $schedulerList = $this->database->getTimeSchedulerList($time);
            // 获取可执行的调度器数据
            foreach ($schedulerList as $key => $scheduler) {
                try {
                    // 修改调度器状态为禁用
                    if (!$this->database->disableScheduler($scheduler['id'])) {
                        unset($schedulerList[$key]);
                    }
                } catch (Exception $exception) {
                    self::recordLog($exception);
                }
            }
            // 将数据插入到分发器表中
            $this->database->insertAllDistributorList($schedulerList);
        } catch (Exception $exception) {
            self::recordLog($exception);
        }
    }

    /**
     * 分发器
     */
    private function taskDistributor(): void
    {
        try {
            // 获取分发器数据
            $distributorList = $this->database->getDistributorList();
            // 获取队列的数据
            foreach ($distributorList as $distributor) {
                try {
                    $taskList = self::jsonDecode($distributor['task_data']) ?: [[]];
                    // 修改分发器状态为执行中
                    if ($this->database->startDistributor($distributor['id'], count($taskList))) {
                        $this->database->insertAllQueueList($distributor['id'], $taskList, $distributor['task_topic']);
                        // 修改分发器状态为成功
                        $this->database->endDistributor($distributor['id'], 3, '执行成功');
                    }
                } catch (Exception $exception) {
                    // 修改分发器状态为失败
                    $this->database->endDistributor($distributor['id'], 4, $exception->getMessage());
                    self::recordLog($exception);
                }
            }
        } catch (Exception $exception) {
            self::recordLog($exception);
        }
    }

    /**
     * 队列调度
     * @param TcpConnection $connection
     * @param array $data
     * @param int $time
     */
    private function queueScheduler(TcpConnection $connection, array $data, int $time): void
    {
        try {
            // 获取排除的任务topic
            $excludeTopic = $data['exclude_topic'] ?? [];
            // 查询数量
            $selectCount = $data['select_count'] ?? 50;
            if ($selectCount <= 0) {
                return;
            }
            // 获取队列数据
            $queueList = $this->database->getQueueList($time, $excludeTopic, $selectCount);
            if ($queueList) {
                // 发送给主进程，进程队列分发
                $this->sendMessage($connection, 'queue_distributor', $queueList);
            }
        } catch (Exception $exception) {
            self::recordLog($exception);
        }
    }

    /**
     * 处理逾期任务
     * @param array $ids
     * @param int $time
     */
    private function handleOverdueTasks(array $ids, int $time): void
    {
        try {
            // 获取可重试的任务id
            $retryTaskIds = $this->database->getRetryQueueIds($ids, $time, true);
            // 获取不可重试的任务id
            $notRetryTaskIds = $this->database->getRetryQueueIds($ids, $time, false);
            // 处理可重试的任务
            $this->database->endRetryQueues($retryTaskIds, $time, true);
            // 处理不可重试的任务
            $this->database->endRetryQueues($notRetryTaskIds, $time, false);
            // 处理可重试的任务日志
            $this->database->endRetryQueueLogs($retryTaskIds, $time, true);
            // 处理不可重试的任务日志
            $this->database->endRetryQueueLogs($notRetryTaskIds, $time, false);
        } catch (Exception $exception) {
            self::recordLog($exception);
        }
    }

    /**
     * 执行任务
     * @param array $data
     */
    private function executeTask(array $data): void
    {
        $queueId = $data['queue_id'];
        $taskTopic = $data['task_topic'];
        // 记录正在执行的进程id和次数
        if (isset($this->taskTopicCountPool[$taskTopic])) {
            ++$this->taskTopicCountPool[$taskTopic];
        } else {
            $this->taskTopicCountPool[$taskTopic] = 1;
        }
        $this->taskQueueIdPool[$queueId] = $queueId;
        try {
            // 查询队列信息
            $queueData = $this->database->getQueue($queueId);
            if ($queueData) {
                $taskFile = $data['task_file'] ?? '';
                $maxExecuteCount = $taskFile ? $taskFile::getMaxExecuteCount() : 1;
                // 将队列和队列日志状态改为执行中
                if ($this->database->startQueue($queueId, $maxExecuteCount)) {
                    $this->database->startQueueLog($queueId);
                    if (empty($taskFile)) {
                        // 将队列和队列日志状态改为结束
                        $this->database->endQueue($queueId, time(), 4, '任务主题不存在');
                        $this->database->endQueueLog($queueId, time(), 3, '任务主题不存在');
                    } else {
                        try {
                            if (!isset($this->taskTopicInstancePool[$taskTopic])) {
                                $this->taskTopicInstancePool[$taskTopic] = Container::getInstance()
                                    ->invokeClass($taskFile);
                            }
                            $taskTopicInstance = $this->taskTopicInstancePool[$taskTopic];
                            // 执行任务
                            $result = $taskTopicInstance->run(array_merge($queueData, [
                                'execute_status' => 2,
                                'execute_result' => '',
                                'max_execute_count' => $maxExecuteCount,
                                'start_time' => time(),
                            ]));
                            // 获取执行结果
                            $executeStatus = $result->getExecuteStatus();
                            $executeResult = $result->getExecuteResult();
                            if ($executeStatus === 1 && $queueData['execute_count'] >= ($maxExecuteCount - 1)) {
                                $executeStatus = 4;
                            }
                            // 将队列状态改为结束
                            $this->database->endQueue($queueId, time(), $executeStatus, $executeResult);
                        } catch (Exception $exception) {
                            $executeStatus = ($queueData['execute_count'] >= ($maxExecuteCount - 1)) ? 4 : 1;
                            $executeResult = (string)$exception;
                            // 将队列状态改为结束
                            $this->database->endQueue($queueId, time(), $executeStatus, $executeResult);
                        }
                        // 将队列日志状态改为结束
                        $logStatus = $executeStatus === 1 ? 3 : ($executeStatus - 1);
                        $this->database->endQueueLog($queueId, time(), $logStatus, $executeResult);
                    }
                }
            }
        } catch (Exception $exception) {
            self::recordLog($exception);
        }
        // 清理正在执行的进程id和次数
        --$this->taskTopicCountPool[$taskTopic];
        unset($this->taskQueueIdPool[$queueId]);
    }
}
