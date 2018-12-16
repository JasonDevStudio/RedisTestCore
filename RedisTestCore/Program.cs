using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace RedisTestCore
{
    class Program
    {
        static void Main(string[] args)
        {
            ZSETTEST3();
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

        static void ZSETTEST3()
        {
            Console.WriteLine("管道，先整理好所有KEYVALUE，再一次性写入");
            Console.WriteLine("请输入写入次数：");
            var count = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient("127.0.0.1:6379,defaultDatabase=8,poolsize=100,ssl=false,writeBuffer=1024000");

            var stw = Stopwatch.StartNew();

            var vals = new List<(double, object)>();

            for (int i = 0; i < count; i++)
            {
                vals.Add((i, Guid.NewGuid().ToString()));
            }

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 整理完成，耗时{stw.Elapsed.TotalMilliseconds} 毫秒。。。");

            var page_size = 500000;
            var page_index = 0;
            var total_page = Convert.ToInt32(vals.Count / page_size) + (vals.Count % page_size > 0 ? 1 : 0);

            var stw01 = Stopwatch.StartNew();

            do
            {
                stw.Restart();
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 开始提取。。。");

                var temp_vals = vals.Skip(page_index * page_size).Take(page_size);

                stw.Stop();
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 提取耗时 {stw.ElapsedMilliseconds} 毫秒。。。");

                stw.Restart();
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 开始写入。。。");
                rds.StartPipe(p => rds.ZAdd("dddddddd", temp_vals.ToArray()));
                stw.Stop();
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 写入完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");

                page_index++;

            } while (total_page > page_index);


            stw01.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {count} 次写入完成，耗时{stw01.Elapsed.TotalMilliseconds} 毫秒");
        }

        static void HASHTEST0()
        {
            Console.WriteLine("HashMap,管道，先整理好所有KEYVALUE，再一次性写入");
            Console.WriteLine("请输入写入次数：");
            var count = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient("127.0.0.1:6379,defaultDatabase=1,poolsize=100,ssl=false,writeBuffer=1024000");

            var stw = Stopwatch.StartNew();

            var vals = new List<object>();

            for (int i = 0; i < count; i++)
            {
                vals.Add(Guid.NewGuid().ToString());
                vals.Add(i);
            }

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 整理完成，耗时{stw.Elapsed.TotalMilliseconds} 毫秒。。。");

            stw.Restart();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始写入。。。");

            rds.StartPipe(p => rds.HMSet("asdfghj3kl1", vals.ToArray()));

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {count} 次写入完成，耗时{stw.Elapsed.TotalMilliseconds} 毫秒");
        }
    }
}
