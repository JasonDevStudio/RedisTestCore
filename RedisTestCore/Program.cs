using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace RedisTestCore
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("先整理好所有KEYVALUE，再一次性写入");
            Console.WriteLine("请输入写入次数：");
            var count = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient("127.0.0.1:6379,defaultDatabase=11,poolsize=10,ssl=false,writeBuffer=1024000");

            var stw = Stopwatch.StartNew();

            var vals = new List<(double, object)>();

            for (int i = 0; i < count; i++)
            {
                vals.Add((i, Guid.NewGuid().ToString()));
            }

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 整理完成，耗时 {stw.Elapsed.TotalSeconds} 秒");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始写入。。。");

            rds.ZAdd("asdfghjkl", vals.ToArray());

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {count} 次写入完成，耗时{stw.Elapsed.TotalSeconds} 秒");

        }

        static void ZSETTEST1()
        {
            Console.Write("请输入写入次数：");
            var count = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始写入。。。");
            var rds = new CSRedis.CSRedisClient("127.0.0.1:6379,defaultDatabase=11,poolsize=10,ssl=false,writeBuffer=10240");

            var stw = Stopwatch.StartNew();

            for (int i = 0; i < count; i++)
            {
                rds.ZAdd("asdfghjkl", (i, Guid.NewGuid().ToString()));
            }

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {count} 次写入完成，耗时{stw.Elapsed.TotalSeconds} 秒");
        }

        static void ZSETTEST2()
        {
            Console.WriteLine("先整理好所有KEYVALUE，再一次性写入");
            Console.WriteLine("请输入写入次数：");
            var count = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始写入。。。");
            var rds = new CSRedis.CSRedisClient("127.0.0.1:6379,defaultDatabase=11,poolsize=10,ssl=false,writeBuffer=10240");

            var stw = Stopwatch.StartNew();

            var vals = new List<(double, object)>();

            for (int i = 0; i < count; i++)
            {
                vals.Add((i, Guid.NewGuid().ToString()));
            }

            rds.ZAdd("asdfghjkl", vals.ToArray());

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {count} 次写入完成，耗时{stw.Elapsed.TotalSeconds} 秒");
        }
    }
}
