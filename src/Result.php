<?php

namespace ycl123\queue;

use Exception;

final class Result
{
    /**
     * 失败，可重试
     */
    public const FAIL = 1;

    /**
     * 成功
     */
    public const SUCCESS = 3;

    /**
     * 终止，不可重试
     */
    public const FINISH = 4;

    /**
     * 取消
     */
    public const CANCEL = 5;

    /**
     * 执行状态
     * @var int
     */
    private $executeStatus;

    /**
     * 执行结果
     * @var array|string|null
     */
    private $executeResult;

    /**
     * Result constructor.
     * @param int $executeStatus 执行状态
     * @param array|string $executeResult 执行结果
     * @throws Exception
     */
    public function __construct(int $executeStatus, $executeResult = '')
    {
        if (!in_array($executeStatus, [1, 3, 4, 5], false)) {
            throw new Exception('任务执行状态设置异常，期望为：1,3,4,5，实传：' . $executeStatus);
        }
        $this->executeStatus = $executeStatus;
        $this->executeResult = $executeResult;
    }

    /**
     * 获取执行状态
     * @return int
     */
    public function getExecuteStatus(): int
    {
        return $this->executeStatus;
    }

    /**
     * 获取执行结果
     * @return string
     * @throws Exception
     */
    public function getExecuteResult(): string
    {
        try {
            $executeResult = $this->executeResult;
            return is_array($executeResult) ? Start::jsonEncode($executeResult) : (string)$executeResult;
        } catch (Exception $exception) {
            throw new Exception('获取返回数据失败');
        }
    }

    /**
     * 失败，可重试
     * @param array|string $executeResult 执行结果
     * @return Result
     * @throws Exception
     */
    public static function instanceFail($executeResult = '失败'): Result
    {
        return new self(self::FAIL, $executeResult);
    }

    /**
     * 成功
     * @param array|string $executeResult 执行结果
     * @return Result
     * @throws Exception
     */
    public static function instanceSuccess($executeResult = '成功'): Result
    {
        return new self(self::SUCCESS, $executeResult);
    }

    /**
     * 终止，不可重试
     * @param array|string $executeResult 执行结果
     * @return Result
     * @throws Exception
     */
    public static function instanceFinish($executeResult = '终止'): Result
    {
        return new self(self::FINISH, $executeResult);
    }

    /**
     * 取消
     * @param array|string $executeResult 执行结果
     * @return Result
     * @throws Exception
     */
    public static function instanceCancel($executeResult = '取消'): Result
    {
        return new self(self::CANCEL, $executeResult);
    }

    /**
     * 结果
     * @param int $executeStatus 执行状态
     * @param array|string $executeResult 执行结果
     * @return Result
     * @throws Exception
     */
    public static function instanceResult(int $executeStatus, $executeResult = ''): Result
    {
        return new self($executeStatus, $executeResult);
    }
}
