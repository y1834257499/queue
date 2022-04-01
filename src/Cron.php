<?php

namespace ycl123\queue;

final class Cron
{
    /**
     * 是否是cron执行时间
     * @param $cron
     * @param int|null $time
     * @return bool
     */
    public static function isExecuteCronTime($cron, int $time = null): bool
    {
        if (!self::isValidCron($cron)) {
            return false;
        }
        $parseData = self::parseCron($cron, $time);
        return ($parseData['second'] === true || in_array((int)date('s', $time), $parseData['second'], false))
            && ($parseData['minute'] === true || in_array((int)date('i', $time), $parseData['minute'], false))
            && ($parseData['hour'] === true || in_array((int)date('G', $time), $parseData['hour'], false))
            && ($parseData['day'] === true || in_array((int)date('j', $time), $parseData['day'], false))
            && ($parseData['month'] === true || in_array((int)date('n', $time), $parseData['month'], false))
            && ($parseData['week'] === true || in_array((int)date('N', $time), $parseData['week'], false))
            && ($parseData['year'] === true || in_array((int)date('Y', $time), $parseData['year'], false));
    }

    /**
     * 解析cron
     * @param string $cron
     *   0    1    2    3    4    5    6
     *   *    *    *    *    *    ?    *
     *   -    -    -    -    -    -    -
     *   |    |    |    |    |    |    |
     *   |    |    |    |    |    |    +----- year (1970 - 2099) [, - * /] 低于7段表示不指定
     *   |    |    |    |    |    +----- day of week (0 - 6) (Sunday=0) [, - * / ? W (0-6)L (0-6)#(1-5)]
     *   |    |    |    |    +----- month (1 - 12) [, - * /]
     *   |    |    |    +------- day of month (1 - 31) [, - * / ? L LW (1-31)W]
     *   |    |    +--------- hour (0 - 23) [, - * /]
     *   |    +----------- minute (0 - 59) [, - * /]
     *   +------------- second (0 - 59) [, - * /] 低于6段表示不指定
     *
     *   ,: 表示列出枚举值
     *   -: 表示范围
     *   *: 表示匹配该域的任意值
     *   /: 表示起始时间开始触发，然后每隔固定时间触发一次
     *   ?: 表示不指定，只能用在day of month和day of week两个域，并且必须也只能有一个为 ?
     *   L: 表示某月的最后一天
     *   W: 表示有效工作日(1-5)
     *   LW: 表示某月的最后一个工作日
     *   (0-6)L: 表示某月的最后一个星期几(0-6)
     *   (1-31)W: 表示离指定日期(1-31)最近的一个工作日，若指定日期大于当月最大日期则不触发
     *   (0-6)#(1-5): 表示某月的第几个(1-5)星期几(0-6)
     * @param int|null $time
     * @return array
     */
    public static function parseCron(string $cron, int $time = null): array
    {
        $cron = explode(' ', $cron);
        $cron_count = count($cron);
        $cron_ = 0;
        return [
            'second' => in_array($cron_count, [6, 7]) ? self::parseCronSegment($cron[$cron_++], 0, 59, $time) : [0],
            'minute' => self::parseCronSegment($cron[$cron_++], 0, 59, $time),
            'hour' => self::parseCronSegment($cron[$cron_++], 0, 23, $time),
            'day' => self::parseCronSegment($cron[$cron_++], 1, 31, $time),
            'month' => self::parseCronSegment($cron[$cron_++], 1, 12, $time),
            'week' => self::parseCronSegment($cron[$cron_++], 1, 7, $time),
            'year' => $cron_count === 7 ? self::parseCronSegment($cron[$cron_], 1970, 2099, $time) : true
        ];
    }

    /**
     * 验证是否是一个cron
     * @param string $cron
     * @return bool
     */
    public static function isValidCron(string $cron): bool
    {
        if (preg_match(
        // 秒分
            '/^(((((([0-5]?\d)?\/)?([0-5]?\d))|(([0-5]?\d)-([0-5]?\d))|\*),)*' .
            '(((([0-5]?\d)?\/)?([0-5]?\d))|(([0-5]?\d)-([0-5]?\d))|\*) ){1,2}' .
            // 小时
            '((((((\d|[01]\d|2[0-3])?\/)?(\d|[01]\d|2[0-3]))|((\d|[01]\d|2[0-3])-(\d|[01]\d|2[0-3]))|\*),)*' .
            '((((\d|[01]\d|2[0-3])?\/)?(\d|[01]\d|2[0-3]))|((\d|[01]\d|2[0-3])-(\d|[01]\d|2[0-3]))|\*) )' .
            '((' .
            // 天
            '(((((([1-9]|10|[12]\d|3[01])?\/)?([1-9]|10|[12]\d|3[01]))|\*|LW?|([1-9]|10|[12]\d|3[01])W' .
            '|(([1-9]|10|[12]\d|3[01])-([1-9]|10|[12]\d|3[01]))),)*' .
            '(((([1-9]|10|[12]\d|3[01])?\/)?([1-9]|10|[12]\d|3[01]))|\*|LW?|([1-9]|10|[12]\d|3[01])W' .
            '|(([1-9]|10|[12]\d|3[01])-([1-9]|10|[12]\d|3[01]))) )' .
            // 月
            '(((((([1-9]|1[0-2])?\/)?([1-9]|1[0-2]))|(([1-9]|1[0-2])-([1-9]|1[0-2]))|\*),)*' .
            '(((([1-9]|1[0-2])?\/)?([1-9]|1[0-2]))|(([1-9]|1[0-2])-([1-9]|1[0-2]))|\*) )' .
            // 星期
            '\?' .
            ')|(' .
            // 天
            '\? ' .
            // 月
            '(((((([1-9]|1[0-2])?\/)?([1-9]|1[0-2]))|(([1-9]|1[0-2])-([1-9]|1[0-2]))|\*),)*' .
            '(((([1-9]|1[0-2])?\/)?([1-9]|1[0-2]))|(([1-9]|1[0-2])-([1-9]|1[0-2]))|\*) )' .
            // 星期
            '((((([0-6]?\/)?[0-6])|([0-6]-[0-6])|\*|W|[0-6](L|#[1-5])),)*' .
            '((([0-6]?\/)?[0-6])|([0-6]-[0-6])|\*|W|[0-6](L|#[1-5])))' .
            '))' .
            // 年
            '( (((((19[7-9]\d|20\d{2})?\/)?(19[7-9]\d|20\d{2}))|((19[7-9]\d|20\d{2})-(19[7-9]\d|20\d{2}))|\*),)*' .
            '((((19[7-9]\d|20\d{2})?\/)?(19[7-9]\d|20\d{2}))|((19[7-9]\d|20\d{2})-(19[7-9]\d|20\d{2}))|\*))?$/',
            trim($cron)
        )) {
            return true;
        }
        return false;
    }

    /**
     * 解析cron段
     * @param string $cron
     * @param int $min
     * @param int $max
     * @param int|null $time
     * @return array|true
     */
    private static function parseCronSegment(string $cron, int $min, int $max, int $time = null)
    {
        if ($cron === '?') {
            return true;
        }
        $segments = explode(',', $cron);
        if (in_array('*', $segments, false)) {
            return true;
        }
        $between = [];
        if (null === $time) {
            $time = time();
        }
        foreach ($segments as $segment) {
            $segment = ltrim($segment, '0');
            if (strpos($segment, '-') !== false) {
                [$min_, $max_] = explode('-', $segment);
                if ($min_ > $max_) {
                    for ($i = $min_; $i <= $max; $i++) {
                        $between[] = $i;
                    }
                    for ($i = $min; $i <= $max_; $i++) {
                        $between[] = $i;
                    }
                } else {
                    for ($i = $min_; $i <= $max_; $i++) {
                        $between[] = $i;
                    }
                }
            } elseif (strpos($segment, '/') !== false) {
                [$start, $step] = explode('/', $segment);
                $start = $start === '' ? $min : $start;
                for ($i = $start; $i <= $max; $i += $step) {
                    $between[] = $i;
                }
            } elseif ($segment === 'W') {
                for ($i = 1; $i <= 5; $i++) {
                    $between[] = $i;
                }
            } elseif ($segment === 'L') {
                $between[] = date('t', $time);
            } elseif ($segment === 'LW') {
                $ymt = date('Y-m-t', $time);
                $week = (int)date('N', strtotime($ymt));
                $less = 5 - $week;
                $between[] = $less < 0 ? date('j', strtotime($less . ' day ' . $ymt)) : date('t', $time);
            } elseif (strpos($segment, 'W') !== false && strpos($segment, 'W', -1) === 1) {
                $day = (int)substr($segment, 0, -1);
                $t = (int)date('t', $time);
                if ($day <= $t) {
                    $ymt = date('Y-m-' . $day);
                    $week = (int)date('N', strtotime($ymt));
                    if ($week <= 5) {
                        $between[] = $day;
                    } elseif ($week === 6) {
                        if ($day === 1) {
                            $between[] = $day + 2;
                        } else {
                            $between[] = $day - 1;
                        }
                    } elseif ($day === $t) {
                        $between[] = $day - 2;
                    } else {
                        $between[] = $day + 1;
                    }
                }
            } elseif (strpos($segment, 'W') !== false && strpos($segment, 'L', -1) === 1) {
                $week = (int)substr($segment, 0, -1);
                $week = $week === 0 ? 7 : $week;
                $ymt = date('Y-m-t', $time);
                $lastWeek = (int)date('N', strtotime($ymt));
                if ($week === $lastWeek) {
                    $between[] = date('t', $time);
                } elseif ($week < $lastWeek) {
                    $between[] = date('j', strtotime('-' . ($lastWeek - $week) . ' day ' . $ymt));
                } else {
                    $between[] = date('j', strtotime('+' . ($week - $lastWeek) . ' day -1 week' . $ymt));
                }
            } elseif (strpos($segment, '#') !== false) {
                [$week, $n] = explode('#', $segment);
                [$week, $n] = [(int)$week, (int)$n];
                $week = $week === 0 ? 7 : $week;
                $startDay = $n * 7 - 6;
                $week_ = (int)date('N', strtotime(date('Y-m-' . $startDay)));
                $key = 0;
                do {
                    if ($week_ === $week) {
                        $day = $startDay + $key;
                        if ($day <= (int)date('t', $time)) {
                            $between[] = $day;
                        }
                        break;
                    }
                    $key++;
                    $week_ = ++$week_ > 7 ? 1 : $week_;
                } while ($key <= 6);
            } else {
                $between[] = $segment;
            }
        }
        return $between;
    }
}
