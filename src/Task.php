<?php

namespace ycl123\queue;

abstract class Task
{
    /**
     * 任务处理逻辑
     * @param array $taskData
     * @return Result
     */
    abstract public function run(array $taskData): Result;

    /**
     * 任务主题
     * @return string
     */
    abstract public static function getTaskTopic(): string;

    /**
     * 获得最大并发
     * @return int
     */
    public static function getMaxConcurrent(): int
    {
        return 5;
    }

    /**
     * 获取最大执行次数
     * @return int
     */
    public static function getMaxExecuteCount(): int
    {
        return 5;
    }
}
