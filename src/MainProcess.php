<?php

namespace ycl123\queue;

use Exception;
use ReflectionClass;
use Symfony\Component\Finder\Finder;
use Workerman\Connection\AsyncTcpConnection;
use Workerman\Timer;
use Workerman\Worker;

final class MainProcess
{
    /**
     * @var array 配置
     */
    private $config;

    /**
     * @var AsyncTcpConnection[] 异步tcp连接池
     *  ['workerId' => $connection]
     */
    private $asyncTcpConPool = [];

    /**
     * @var int[] 异步tcp连接id池
     *  ['connectionId' => $workerId]
     */
    private $asyncTcpConIdPool = [];

    /**
     * @var int[] 异步tcp连接池的队列数
     *  ['workerId' => $queueCount]
     */
    private $asyncTcpConPoolQueueCount = [];

    /**
     * @var int 已建立的异步tcp连接数量
     */
    private $asyncTcpConCount = 0;

    /**
     * @var int[][] 异步tcp连接池的任务数
     *  ['workerId' => ['task_topic' => $taskTopicCount]]
     */
    private $asyncTcpConPoolTaskCount = [];

    /**
     * @var int[] 任务主题数量池
     * ['task_topic' => $taskTopicCount]
     */
    private $taskTopicCountPool = [];

    /**
     * @var int[] 池中的任务最大并发数
     * ['task_topic' => $taskTopicMaxConcurrent]
     */
    private $taskTopicMaxConcurrentPool = [];

    /**
     * @var Task[]|string[] 任务主题文件池
     * ['task_topic' => $taskTopicFile]
     */
    private $taskTopicFilePool = [];

    /**
     * @var string[] 达到最大并发数的topic
     */
    private $maxConcurrentTaskTopic = [];

    /**
     * @var int[][] 异步tcp连接池的任务id
     *  ['workerId' => ['queue_id' => $queueId]]
     */
    private $asyncTcpConPoolTaskQueueId = [];

    /**
     * @var int[] 任务队列id池
     * ['queue_id' => $queueId]
     */
    private $taskQueueIdPool = [];

    /**
     * @var array[] 未发送的消息
     * [$sendData, $sendData]
     */
    private $unsentMessages = [];

    /**
     * @var array[] 未处理的任务队列
     * ['queue_id' => $queueData]
     */
    private $untreatedTaskQueue = [];

    public function __construct(array $config)
    {
        $this->config = $config;
    }

    public function run(): void
    {
        // 加载任务文件
        $this->loadTaskFile();
        // 建立异步tcp连接
        $this->buildAsyncTcpCon();
        // 设置定时器
        $this->setTimer();
    }

    /**
     * 加载任务文件
     */
    private function loadTaskFile(): void
    {
        try {
            foreach ($this->config['task_dirs'] as $namespace => $taskDir) {
                $taskFiles = (new Finder())->in($taskDir)->name('*.php')->files();
                foreach ($taskFiles as $taskFile) {
                    $taskFile = trim($namespace, '\\|/') . '\\' . str_replace(
                            ['//', '/', '.php'],
                            ['\\', '\\', ''],
                            trim(array_reverse(explode($taskDir, $taskFile->getPathname(), 2))[0], '\\|/')
                        );
                    if (is_subclass_of($taskFile, Task::class) && !(new ReflectionClass($taskFile))->isAbstract()) {
                        $taskTopic = $taskFile::getTaskTopic();
                        $this->taskTopicFilePool[$taskTopic] = $taskFile;
                        $this->taskTopicCountPool[$taskTopic] = $taskFile::getMaxConcurrent();
                    }
                }
            }
            foreach ($this->config['task_class'] as $taskFile) {
                if (is_subclass_of($taskFile, Task::class) && !(new ReflectionClass($taskFile))->isAbstract()) {
                    $taskTopic = $taskFile::getTaskTopic();
                    $this->taskTopicFilePool[$taskTopic] = $taskFile;
                    $this->taskTopicCountPool[$taskTopic] = $taskFile::getMaxConcurrent();
                }
            }
        } catch (Exception $exception) {
            Start::recordLog($exception);
        }
    }

    /**
     * 建立异步tcp连接
     */
    public function buildAsyncTcpCon(): void
    {
        try {
            // 如果已建立的异步tcp连接大于等于设置的进程数则停止建立新的异步tcp连接
            if ($this->asyncTcpConCount >= $this->config['count']) {
                return;
            }
            $this->asyncTcpConCount++;
            // 建立异步tcp连接
            $remoteAddress = 'text://' . $this->config['socket'];
            $contextOption = $this->config['con_content'] ?? [];
            $asyncTcpCon = new AsyncTcpConnection($remoteAddress, $contextOption);
            $asyncTcpCon->onMessage = [$this, 'onMessage'];
            $asyncTcpCon->onClose = [$this, 'onClose'];
            $asyncTcpCon->connect();
            // 如果已建立的异步tcp连接小于设置的进程数则建立新的异步tcp连接
            if ($this->asyncTcpConCount < $this->config['count']) {
                $this->buildAsyncTcpCon();
            }
        } catch (Exception $exception) {
            $this->asyncTcpConCount--;
            Start::recordLog($exception);
        }
    }

    /**
     * 设置定时器
     */
    public function setTimer(): void
    {
        // 建立异步tcp连接
        Timer::add(1, function () {
            $this->buildAsyncTcpCon();
        });
        // 设置调度定时器
        $timers = ['cron_scheduler', 'time_scheduler', 'distributor', 'queue', 'overdue'];
        foreach ($timers as $timer) {
            $timeInterval = $this->config[$timer . '_time_interval'];
            if ($timeInterval) {
                Timer::add($timeInterval, function () use ($timer) {
                    switch ($timer) {
                        case 'cron_scheduler':
                            $this->sendMessage('cron_scheduler');
                            break;
                        case 'time_scheduler':
                            $this->sendMessage('time_scheduler');
                            break;
                        case 'distributor':
                            $this->sendMessage('task_distributor');
                            break;
                        case 'queue':
                            // 发送队列调度消息
                            $this->sendQueueSchedulerMessages();
                            break;
                        case 'overdue':
                            $this->sendMessage('overdue_tasks', array_values($this->taskQueueIdPool));
                            break;
                    }
                });
            }
        }
    }

    /**
     * 收到消息
     * @param AsyncTcpConnection $connection
     * @param $receiveData
     */
    public function onMessage(AsyncTcpConnection $connection, $receiveData): void
    {
        try {
            if (empty($receiveData = Start::jsonDecode($receiveData))) {
                return;
            }
            $type = (string)$receiveData['type'];
            $workerId = (int)($receiveData['worker_id']);
            $data = (array)$receiveData['data'];
            switch ($type) {
                // 连接建立
                case 'connect':
                    $this->connectEstablish($connection, $workerId, $data);
                    break;
                // 队列分发
                case 'queue_distributor':
                    $this->queueDistributor($data);
                    break;
                // 任务处理完成
                case 'finished':
                    // 处理进程队列数量
                    if (isset($this->asyncTcpConPoolQueueCount[$workerId])) {
                        --$this->asyncTcpConPoolQueueCount[$workerId];
                    }
                    break;
                // 执行任务完成
                case 'execute_task_finished':
                    $this->executeTaskFinished($workerId, $data);
                    $this->queueDistributor();
                    break;
            }
        } catch (Exception $exception) {
            Start::recordLog($exception);
        }
    }

    /**
     * 关闭连接
     * @param AsyncTcpConnection $connection
     */
    public function onClose(AsyncTcpConnection $connection): void
    {
        $this->asyncTcpConCount--;
        if (isset($this->asyncTcpConIdPool[$connection->id])) {
            $workerId = $this->asyncTcpConIdPool[$connection->id];
            // 处理任务数
            $taskTopicCountPool = $this->asyncTcpConPoolTaskCount[$workerId] ?? [];
            foreach ($taskTopicCountPool as $taskTopic => $taskTopicCount) {
                // 处理任务主题数量
                if (isset($this->taskTopicCountPool[$taskTopic])) {
                    $this->taskTopicCountPool[$taskTopic] -= $taskTopicCount;
                }
                // 处理达到最大并发数的任务主题
                $this->handleMaxConcurrentTaskTopic($taskTopic);
            }
            // 处理任务队列id
            $taskQueueIdPool = $this->asyncTcpConPoolTaskQueueId[$workerId] ?? [];
            foreach ($taskQueueIdPool as $queueId) {
                unset($this->taskQueueIdPool[$queueId]);
            }

            // 删除跟当前worker连接相关的信息
            unset(
                $this->asyncTcpConPool[$workerId],
                $this->asyncTcpConIdPool[$connection->id],
                $this->asyncTcpConPoolQueueCount[$workerId],
                $this->asyncTcpConPoolTaskCount[$workerId],
                $this->asyncTcpConPoolTaskQueueId[$workerId],
                $taskTopicCountPool,
                $taskQueueIdPool
            );
        }
        // 重新建立异步tcp连接
        $this->buildAsyncTcpCon();
    }

    /**
     * 发送消息
     * @param string $type
     * @param array $data
     * @param int $time
     * @param null|callable $callback
     */
    private function sendMessage(string $type, array $data = [], int $time = 0, $callback = null): void
    {
        try {
            $time = $time ?: time();
            $sendData = ['type' => $type, 'data' => $data, 'time' => $time];
            // 判断连接池是否为空
            if (empty($this->asyncTcpConPool)) {
                // 记录未发送的消息
                $this->unsentMessages[] = $sendData;
                return;
            }
            // 获取任务数最少的异步tcp连接
            asort($this->asyncTcpConPoolQueueCount);
            $workerId = key($this->asyncTcpConPoolQueueCount);
            // 获取连接，发送消息
            $connection = $this->asyncTcpConPool[$workerId];
            if ($connection->send(Start::jsonEncode($sendData))) {
                // 记录连接队列数
                ++$this->asyncTcpConPoolQueueCount[$workerId];
                if (is_callable($callback)) {
                    $callback($workerId);
                }
            }
        } catch (Exception $exception) {
            Start::recordLog($exception);
        }
    }

    /**
     * 发送未发送的消息
     */
    private function sendUnsentMessages(): void
    {
        $unsentMessages = $this->unsentMessages;
        $this->unsentMessages = [];
        foreach ($unsentMessages as $unsentMessage) {
            $this->sendMessage($unsentMessage['type'], $unsentMessage['data'], $unsentMessage['time']);
        }
    }

    /**
     * 发送队列调度消息
     */
    private function sendQueueSchedulerMessages(): void
    {
        $sumTaskTopicCount = array_sum($this->taskTopicCountPool);
        if ($this->config['task_concurrent'] > $sumTaskTopicCount) {
            $this->sendMessage('queue_scheduler', [
                'exclude_topic' => array_values($this->maxConcurrentTaskTopic),
                'select_count' => $this->config['task_concurrent'] - $sumTaskTopicCount
            ]);
        }
    }

    /**
     * 连接建立
     * @param AsyncTcpConnection $connection
     * @param int $workerId
     * @param array $data
     */
    private function connectEstablish(AsyncTcpConnection $connection, int $workerId, array $data): void
    {
        if (isset($this->asyncTcpConPool[$workerId])) {
            // 如果已存在对应的进程连接，则关闭当前连接
            $connection->close();
        } else {
            $this->asyncTcpConIdPool[$connection->id] = $workerId;
            $this->asyncTcpConPool[$workerId] = $connection;
            $this->asyncTcpConPoolQueueCount[$workerId] = $data['queue_count'];
            // 处理任务数
            $taskTopicCountPool = $data['task_topic_pool_count'];
            $this->asyncTcpConPoolTaskCount[$workerId] = $taskTopicCountPool;
            foreach ($taskTopicCountPool as $taskTopic => $taskTopicCount) {
                if (isset($this->taskTopicCountPool[$taskTopic])) {
                    $this->taskTopicCountPool[$taskTopic] += $taskTopicCount;
                } else {
                    $this->taskTopicCountPool[$taskTopic] = $taskTopicCount;
                }
            }
            // 处理任务队列id
            $taskQueueIdPool = $data['task_queue_id_pool'];
            $this->asyncTcpConPoolTaskQueueId[$workerId] = $taskQueueIdPool;
            foreach ($taskQueueIdPool as $queueId) {
                $this->taskQueueIdPool[$queueId] = $queueId;
            }
            // 发送未发送的消息
            $this->sendUnsentMessages();
        }
    }

    /**
     * 执行任务完成
     * @param int $workerId
     * @param array $data
     */
    private function executeTaskFinished(int $workerId, array $data): void
    {
        $queueId = $data['queue_id'];
        $taskTopic = $data['task_topic'];
        // 处理进程任务数量
        if (isset($this->asyncTcpConPoolTaskCount[$workerId][$taskTopic])) {
            --$this->asyncTcpConPoolTaskCount[$workerId][$taskTopic];
        }
        // 删除正在执行的任务id
        unset($this->asyncTcpConIdPool[$workerId][$queueId], $this->taskQueueIdPool[$queueId]);
        // 处理任务主题数量
        if (isset($this->taskTopicCountPool[$taskTopic])) {
            --$this->taskTopicCountPool[$taskTopic];
        }
        // 处理达到最大并发数的任务主题
        $this->handleMaxConcurrentTaskTopic($taskTopic);
    }

    /**
     * 处理达到最大并发数的任务主题
     * @param string $taskTopic
     */
    private function handleMaxConcurrentTaskTopic(string $taskTopic): void
    {
        // 获取任务主题最大并发数
        $taskTopicMaxConcurrent = $this->getTaskTopicMaxConcurrent($taskTopic);
        // 处理任务主题数量
        if (!isset($this->taskTopicCountPool[$taskTopic])) {
            $this->taskTopicCountPool[$taskTopic] = 0;
        }
        // 处理达到最大并发数的任务主题
        if ($this->taskTopicCountPool[$taskTopic] < $taskTopicMaxConcurrent) {
            unset($this->maxConcurrentTaskTopic[$taskTopic]);
        } elseif (!isset($this->maxConcurrentTaskTopic[$taskTopic])) {
            $this->maxConcurrentTaskTopic[$taskTopic] = $taskTopic;
        }
    }

    /**
     * 获取任务主题最大并发数
     * @param string $taskTopic
     * @return int
     */
    private function getTaskTopicMaxConcurrent(string $taskTopic): int
    {
        // 获取任务主题最大并发数
        $taskTopicMaxConcurrent = $this->taskTopicMaxConcurrentPool[$taskTopic] ?? $this->config['task_topic_concurrent'];
        if ($taskTopicMaxConcurrent > $this->config['task_topic_concurrent']) {
            $taskTopicMaxConcurrent = $this->config['task_topic_concurrent'];
        }
        return $taskTopicMaxConcurrent;
    }

    /**
     * 获取可执行任务队列列表
     * @return array|array[]
     */
    private function getExecutableTaskQueueList(): array
    {
        return array_filter($this->untreatedTaskQueue, function ($item) {
            return !in_array($item, $this->maxConcurrentTaskTopic, false);
        });
    }

    /**
     * 队列分发
     * @param array $data
     */
    private function queueDistributor(array $data = []): void
    {
        if (!empty($data)) {
            $this->untreatedTaskQueue = array_column(array_merge($this->untreatedTaskQueue, $data), null, 'queue_id');
        }
        // 获取可执行的任务队列列表
        $taskQueueList = $this->getExecutableTaskQueueList();
        foreach ($taskQueueList as $queue) {
            $taskTopic = $queue['task_topic'];
            // 获取任务主题最大并发数
            $taskTopicMaxConcurrent = $this->getTaskTopicMaxConcurrent($taskTopic);
            // 处理任务数
            if (!isset($this->taskTopicCountPool[$taskTopic])) {
                $this->taskTopicCountPool[$taskTopic] = 0;
            }
            // 判断任务是否可执行
            if ($this->taskTopicCountPool[$taskTopic] >= $taskTopicMaxConcurrent) {
                $this->handleMaxConcurrentTaskTopic($taskTopic);
                continue;
            }
            // 发送执行任务的消息
            $queueId = $queue['queue_id'];
            $queue['task_file'] = $this->taskTopicFilePool[$taskTopic] ?? '';
            $this->sendMessage('execute_task', $queue, 0, function ($workerId) use ($queueId, $taskTopic) {
                // 删除未处理的消息队列
                unset($this->untreatedTaskQueue[$queueId]);
                // 记录进程任务数
                if (!isset($this->asyncTcpConPoolTaskCount[$workerId][$taskTopic])) {
                    $this->asyncTcpConPoolTaskCount[$workerId][$taskTopic] = 1;
                } else {
                    $this->asyncTcpConPoolTaskCount[$workerId][$taskTopic]++;
                }
                // 记录任务数
                $this->taskTopicCountPool[$taskTopic]++;
                // 记录队列id
                $this->taskQueueIdPool[$queueId] = $queueId;
                $this->asyncTcpConPoolTaskQueueId[$workerId][$queueId] = $queueId;
                // 处理达到最大并发数的任务主题
                $this->handleMaxConcurrentTaskTopic($taskTopic);
            });
        }
        // 发送队列调度消息
        if (!empty($taskQueueList) && !$this->getExecutableTaskQueueList()) {
            $this->sendQueueSchedulerMessages();
        }
    }
}
