<?php
// +----------------------------------------------------------------------
// | ycl123 queue配置
// +----------------------------------------------------------------------

return [
    // socket 上下文选项
    'context' => [],
    // AsyncTcpConnection 上下文选项
    'con_content' => [],
    // 监听地址
    'socket' => '127.0.0.1:9889',
    // 进程数
    'count' => 1,
    // 最大任务并发数
    'task_concurrent' => 50,
    // 最大任务主题并发数
    'task_topic_concurrent' => 10,
    // 任务重试时间间隔(秒)
    'task_retry_time_interval' => 10,
    // 任务超时过期时间(秒)
    'task_timeout_expired_time' => 60,
    // Worker实例的名称
    'name' => 'Ycl123 queue service',
    // 以守护进程方式(-d启动)运行时终端的输出重定向文件路径
    'stdoutFile' => '/dev/null',
    // workerman进程的pid文件路径
    'pidFile' => __DIR__ . '/../worker_ycl123_queue.pid',
    // workerman日志文件位置
    'logFile' => __DIR__ . '/../worker_ycl123_queue.log',
    // 当前Worker实例是否可以reload
    'reloadable' => true,
    // 是否以daemon(守护进程)方式运行
    'daemonize' => false,
    // 针对tp项目，connection为connections中的配置的键名，针对非tp项目connections为connections中的键值，参数请参考tp文档
    'connection' => [],
    // 是否开启cron调度器，开启后每秒调度一次
    'is_cron_scheduler' => true,
    // 时间调度器执行时间间隔(秒)，设置为0则不进行调度，支持小数
    'time_scheduler_time_interval' => 1,
    // 分发器执行时间间隔(秒)，设置为0则不执行分发，支持小数
    'distributor_time_interval' => 1,
    // 队列调度执行时间间隔(秒)，设置为0则不进行调度，支持小数
    'queue_time_interval' => 1,
    // 处理逾期任务时间间隔(秒)，设置为0则不进行处理，支持小数
    'overdue_time_interval' => 30,
    // 日志记录回调，不设置将使用worker的日志记录
    'log' => null,
    // 任务目录定义，会自动扫描目录下的所有任务类 ['namespace' => 'dir']
    'task_dirs' => [],
    // 任务类定义 [Aa::class, Bb::class]
    'task_class' => [],
];
