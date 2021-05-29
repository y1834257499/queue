基于 Workerman4 的定时任务队列。
===============

[![Total Downloads](https://poser.pugx.org/ycl123/queue/downloads)](https://packagist.org/packages/ycl123/queue)
[![Latest Stable Version](https://poser.pugx.org/ycl123/queue/v/stable)](https://packagist.org/packages/ycl123/queue)
[![PHP Version](https://img.shields.io/badge/php-%3E%3D7.2-8892BF.svg)](http://www.php.net/)
[![License](https://poser.pugx.org/ycl123/queue/license)](https://packagist.org/packages/ycl123/queue)

## 参考

[think-queue](https://github.com/top-think/think-queue)  
[workerman-queue](https://github.com/walkor/workerman-queue)

## 主要特性

* 支持cron定时执行
* 支持指定时间执行

## composer 安装

```
composer require ycl123/queue
```

## 配置

|   键  |   默认值  |   类型  |   说明  |
|  ----  |  ----  |  ----  |  ----  |
|  context  |  []  |  array  |  socket 上下文选项 [参考](http://doc.workerman.net/worker/construct.html)  |
|  con_content  |  []  |  array  |  AsyncTcpConnection 上下文选项 [参考](http://doc.workerman.net/async-tcp-connection/construct.html)  |
|  socket  |  127.0.0.1:9889  |  string  |  监听地址 [参考](http://doc.workerman.net/worker/construct.html)  |
|  count  |  1  |  int  |  进程数 [参考](http://doc.workerman.net/worker/count.html)  |
|  task_concurrent  |  50  |  int  |  最大任务并发数  |
|  task_topic_concurrent  |  10  |  int  |  最大任务主题并发数  |
|  task_retry_time_interval  |  10  |  int  |  任务重试时间间隔(秒)  |
|  task_timeout_expired_time  |  60  |  int  |  任务超时过期时间(秒)  |
|  name  |  Ycl123 queue service  |  string  |  Worker实例的名称 [参考](http://doc.workerman.net/worker/name.html)  |
|  stdoutFile  |  /dev/null  |  string  |  以守护进程方式(-d启动)运行时终端的输出重定向文件路径 [参考](http://doc.workerman.net/worker/stdout-file.html)  |
|  pidFile  |  \_\_DIR\_\_ . '/../worker_ycl123_queue.pid'  |  string  |  workerman进程的pid文件路径 [参考](http://doc.workerman.net/worker/pid-file.html)  |
|  logFile  |  \_\_DIR\_\_ . '/../worker_ycl123_queue.log'  |  string  |  workerman日志文件位置 [参考](http://doc.workerman.net/worker/log-file.html)  |
|  reloadable  |  true  |  bool  |  当前Worker实例是否可以reload [参考](http://doc.workerman.net/worker/reloadable.html)  |
|  daemonize  |  false  |  bool  |  是否以daemon(守护进程)方式运行 [参考](http://doc.workerman.net/worker/daemonize.html)  |
|  connection  |  []  |  array：<font color=red>非thinkphp项目</font><br/>string：<font color=red>thinkphp项目</font>  |  array：connections中的键值<br/>string：connections中的键名<br/>[ThinkORM开发指南](https://www.kancloud.cn/manual/think-orm) <br/>[数据库配置](https://www.kancloud.cn/manual/think-orm/1257999) |
|  is_cron_scheduler  |  true  |  bool  |  是否开启cron调度器，开启后每秒调度一次  |
|  time_scheduler_time_interval  |  1  |  int&#124;float  |  时间调度器执行时间间隔(秒)，设置为0则不进行调度，支持小数  |
|  distributor_time_interval  |  1  |  int&#124;float  |  分发器执行时间间隔(秒)，设置为0则不执行分发，支持小数  |
|  queue_time_interval  |  1  |  int&#124;float  |  队列调度执行时间间隔(秒)，设置为0则不进行调度，支持小数  |
|  overdue_time_interval  |  30  |  int&#124;float  |  处理逾期任务时间间隔(秒)，设置为0则不进行处理，支持小数  |
|  log  |  null  |  null&#124;callable(Exception&#124;string $exception)  |  日志记录回调，不设置将使用worker的日志记录  |
|  task_dirs  |  []  |  array  |  任务目录定义，会自动扫描目录下的所有任务类<br/>key：目录的命名空间<br/>value：绝对路径<br/>例：\['app\command' => '/www/wwwroot/tp6/app/command'\]  |
|  task_class  |  []  |  array  |  任务类定义<br/>例：\[Aa::class, Bb::class\]  |

## cron规则

```
0    1    2    3    4    5    6
*    *    *    *    *    ?    *
-    -    -    -    -    -    -
|    |    |    |    |    |    |
|    |    |    |    |    |    +----- year (1970 - 2099) [, - * /] 低于7段表示不指定
|    |    |    |    |    +----- day of week (0 - 6) (Sunday=0) [, - * / ? W (0-6)L (0-6)#(1-5)]
|    |    |    |    +----- month (1 - 12) [, - * /]
|    |    |    +------- day of month (1 - 31) [, - * / ? L LW (1-31)W]
|    |    +--------- hour (0 - 23) [, - * /]
|    +----------- minute (0 - 59) [, - * /]
+------------- second (0 - 59) [, - * /] 低于6段表示不指定

,: 表示列出枚举值
-: 表示范围
*: 表示匹配该域的任意值
/: 表示起始时间开始触发，然后每隔固定时间触发一次
?: 表示不指定，只能用在day of month和day of week两个域，并且必须也只能有一个为 ?
L: 表示某月的最后一天
W: 表示有效工作日(1-5)
LW: 表示某月的最后一个工作日
(0-6)L: 表示某月的最后一个星期几(0-6)
(1-31)W: 表示离指定日期(1-31)最近的一个工作日，若指定日期大于当月最大日期则不触发
(0-6)#(1-5): 表示某月的第几个(1-5)星期几(0-6)
```

## 表结构

```mysql
CREATE TABLE `task_scheduler`
(
    `id`               int(11) unsigned    NOT NULL AUTO_INCREMENT COMMENT '主键',
    `scheduler_type`   tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT '任务类型:1=cron,2=time',
    `scheduler_status` tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT '任务状态:1=正常,2=禁用',
    `scheduler_rule`   varchar(100)        NOT NULL DEFAULT '' COMMENT '调度器规则',
    `task_topic`       varchar(100)        NOT NULL DEFAULT '' COMMENT '任务主题',
    `task_data`        json                         DEFAULT NULL COMMENT '任务数据',
    `create_time`      int(10) unsigned    NOT NULL DEFAULT '0' COMMENT '创建时间',
    `update_time`      int(10) unsigned    NOT NULL DEFAULT '0' COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `idx_search1` (`scheduler_status`, `scheduler_type`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='调度器';

CREATE TABLE `task_distributor`
(
    `id`             int(11) unsigned    NOT NULL AUTO_INCREMENT COMMENT '主键',
    `scheduler_id`   int(11)             NOT NULL DEFAULT '0' COMMENT '调度器id',
    `task_topic`     varchar(100)        NOT NULL DEFAULT '' COMMENT '任务主题',
    `task_data`      json                         DEFAULT NULL COMMENT '任务数据',
    `task_count`     int(7) unsigned     NOT NULL DEFAULT '0' COMMENT '任务数量',
    `execute_status` tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT '执行状态:1=待执行,2=执行中,3=执行成功,4=执行失败,5=已取消',
    `execute_result` text COMMENT '执行结果',
    `create_time`    int(10) unsigned    NOT NULL DEFAULT '0' COMMENT '创建时间',
    `start_time`     int(10) unsigned    NOT NULL DEFAULT '0' COMMENT '开始时间',
    `end_time`       int(10) unsigned    NOT NULL DEFAULT '0' COMMENT '结束时间',
    PRIMARY KEY (`id`),
    KEY `idx_search1` (`execute_status`),
    KEY `idx_search2` (`task_topic`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='分发器';

CREATE TABLE `task_queue`
(
    `id`                int(11) unsigned    NOT NULL AUTO_INCREMENT COMMENT '主键',
    `distributor_id`    int(11)             NOT NULL DEFAULT '0' COMMENT '分发器id',
    `task_topic`        varchar(100)        NOT NULL DEFAULT '' COMMENT '任务主题',
    `task_data`         json                         DEFAULT NULL COMMENT '任务数据',
    `execute_status`    tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT '执行状态:1=待执行,2=执行中,3=执行成功,4=执行失败,5=已取消',
    `execute_result`    text COMMENT '执行结果',
    `execute_count`     int(5) unsigned     NOT NULL DEFAULT '0' COMMENT '执行次数',
    `max_execute_count` int(5) unsigned     NOT NULL DEFAULT '0' COMMENT '最大执行次数',
    `create_time`       int(10)             NOT NULL DEFAULT '0' COMMENT '创建时间',
    `start_time`        int(10)             NOT NULL DEFAULT '0' COMMENT '开始时间',
    `next_execute_time` int(10)             NOT NULL DEFAULT '0' COMMENT '下次执行时间',
    `end_time`          int(10)             NOT NULL DEFAULT '0' COMMENT '结束时间',
    PRIMARY KEY (`id`),
    KEY `idx_search1` (`execute_status`, `task_topic`),
    KEY `idx_search2` (`distributor_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='队列';

CREATE TABLE `task_queue_log`
(
    `id`             int(11) unsigned    NOT NULL AUTO_INCREMENT COMMENT '主键',
    `queue_id`       int(11)             NOT NULL DEFAULT '0' COMMENT '队列id',
    `execute_status` tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT '执行状态:1=执行中,2=执行成功,3=执行失败,4=已取消',
    `execute_result` text COMMENT '执行结果',
    `start_time`     int(10)             NOT NULL DEFAULT '0' COMMENT '开始时间',
    `end_time`       int(10)             NOT NULL DEFAULT '0' COMMENT '结束时间',
    PRIMARY KEY (`id`),
    KEY `idx_search1` (`queue_id`),
    KEY `idx_search2` (`execute_status`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='队列日志';
```

## 使用

> 新建一个任务执行文件`TestTask.php`

```php
namespace ycl123\queue;

class TestTask extends Task
{
    public function run(array $taskTata): Result
    {
        return Result::instanceSuccess();
    }

    public static function getTaskTopic(): string
    {
        return 'TEST_TASK';
    }
}
```

> 在建建一个启动`test.php`

```php
use ycl123\queue\Start;
// 具体配置项请查看 config.php
$config = [
    'socket' => '127.0.0.1:9889',
    'count' => 1,
    // 非tp项目，tp项目 'connection' => 'mysql',
    'connection' => [ 
        'type' => 'mysql',
        'hostname' => '127.0.0.1',
        'database' => 'database',
        'username' => 'username',
        'password' => 'password',
        'hostport' => '3306',
        'charset' => 'utf8mb4',
        'debug' => true,
    ],
    'task_class' => [TestTask::class],
];
$start = new Start($config);
$start->run();
```

> 在数据中中执行下面的sql

```mysql
INSERT INTO `task_scheduler`
(`scheduler_type`, `scheduler_status`, `scheduler_rule`, `task_topic`, `task_data`, `create_time`, `update_time`)
VALUES (1, 1, '* * * * * ?', 'TEST_TASK', '[]', 1621597020, 1621597020);
```

> 启动，其他启动参数请参考workerman[文档](http://doc.workerman.net/install/start-and-stop.html)

```shell
php test.php start
```

> 最后在`task_queue`和`task_queue_log`表中观察任务的执行情况
