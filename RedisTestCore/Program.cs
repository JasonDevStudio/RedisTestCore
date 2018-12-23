 
namespace RedisTestCore
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            do
            {
                
                Console.WriteLine("请选择需要执行的函数：");
                Console.WriteLine("1：ZSetWriteTest");
                Console.WriteLine("2：ZSetReadTest");
                Console.WriteLine("3：HashWriteTest1");
                Console.WriteLine("4：HashReadTest");
                Console.WriteLine("5：HashReadTaskTest");
                Console.WriteLine("6：HashWriteTaskTest");
                var input = Console.ReadLine();
                switch (input)
                {
                    case "1":
                        ZSetWriteTest();
                        break;
                    case "2":
                        ZSetReadTest();
                        break;
                    case "3":
                        HashWriteTest1();
                        break;
                    case "4":
                        HashReadTest();
                        break;
                    case "5":
                        HashReadTaskTest();
                        break;
                    case "6":
                        HashWriteTaskTest();
                        break;
                    default:
                        break;
                }

                Console.WriteLine();
                Console.WriteLine();
            } while (Console.ReadLine().ToUpper() != "EXIT");
        }

        private static void ZSETTEST1()
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

        static void ZSetWriteTest()
        {
            Console.WriteLine("ZSet 写入测试");
            Console.WriteLine("请输入DB：");
            var db = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入KEY：");
            var key = Console.ReadLine();
            Console.WriteLine("请输入开始值：");
            var start = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入结束值：");
            var end = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient(null, $"127.0.0.1:6379,defaultDatabase={db},poolsize=100,ssl=false,writeBuffer=102400");


            var stw = Stopwatch.StartNew();

            var vals = new List<(double, object)>();

            for (int i = start; i < end; i++)
            {
                vals.Add((i * 10, $"{key}_{i}"));
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
                rds.StartPipe(p => rds.ZAdd(key, temp_vals.ToArray()));
                stw.Stop();
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 写入完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");

                page_index++;

            } while (total_page > page_index);


            stw01.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {end - start} 次写入完成，耗时{stw01.Elapsed.TotalMilliseconds} 毫秒");
        }

        static void ZSetReadTest()
        {
            Console.WriteLine("ZSet 读取测试");
            Console.WriteLine("请输入DB：");
            var db = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入KEY：");
            var key = Console.ReadLine();
            Console.WriteLine("请输入开始值：");
            var start = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入结束值：");
            var end = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient(null, $"127.0.0.1:6379,defaultDatabase={db},poolsize=100,ssl=false,writeBuffer=102400");
            var result = new List<string>();
            var stw = Stopwatch.StartNew();
            for (int i = start; i < end; i++)
            {
                var member = $"{key}_{i}";
                double? a = rds.ZScore(key, member);
                result.Add($"key:{key}      member:{member}      score:{a}");
            }

            stw.Stop();

            foreach (var item in result)
            {
                Console.WriteLine(item);
            }

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {end - start}条数据读取完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");
        }

        static void HashReadTest()
        {
            Console.WriteLine("Hash 读取测试");
            Console.WriteLine("请输入DB：");
            var db = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入KEY：");
            var key = Console.ReadLine();
            Console.WriteLine("请输入开始值：");
            var start = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入结束值：");
            var end = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient(null, $"127.0.0.1:6379,defaultDatabase={db},poolsize=100,ssl=false,writeBuffer=102400");
            var result = new List<string>();
            var stw = Stopwatch.StartNew();
            for (int i = start; i < end; i++)
            {
                var member = $"{key}_{i}";
                var a = rds.HGet<decimal>(key, member);
                result.Add($"key:{key}      member:{member}      value:{a}");
            }

            stw.Stop();

            //foreach (var item in result)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {end - start}条数据读取完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");
        }

        static void HashWriteTest()
        {
            Console.WriteLine("Hash 写入测试");
            Console.WriteLine("请输入DB：");
            var db = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入KEY：");
            var key = Console.ReadLine();
            Console.WriteLine("请输入开始值：");
            var start = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入结束值：");
            var end = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient(null, $"127.0.0.1:6379,defaultDatabase={db},poolsize=100,ssl=false,writeBuffer=102400");


            var stw = Stopwatch.StartNew();

            var vals = new Dictionary<string, int>();

            for (int i = start; i < end; i++)
            {
                vals.Add($"{key}_{i}", i);
            }

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 整理完成，耗时{stw.Elapsed.TotalMilliseconds} 毫秒。。。");

            var page_size = 500000;
            var page_index = 0;
            var total_page = Convert.ToInt32(vals.Count / page_size) + (vals.Count % page_size > 0 ? 1 : 0);

            var stw01 = Stopwatch.StartNew();

            //do
            // {
            stw.Restart();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 开始提取。。。");

            var temp_vals = vals.Skip(page_index * page_size).Take(page_size);

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 提取耗时 {stw.ElapsedMilliseconds} 毫秒。。。");

            stw.Restart();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 开始写入。。。");

            foreach (var dic in vals)
            {
                var isexis = rds.HExists(key, dic.Key);

                if (!isexis)
                {
                    rds.HSet(key, dic.Key, dic.Value);
                }
            }

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis Page {page_index} 写入完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");

            page_index++;

            //} while (total_page > page_index);


            stw01.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {end - start} 次写入完成，耗时{stw01.Elapsed.TotalMilliseconds} 毫秒");
        }

        static void HashWriteTest1()
        {
            Console.WriteLine("Hash 写入测试");
            Console.WriteLine("请输入DB：");
            var db = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入KEY：");
            var key = Console.ReadLine();
            Console.WriteLine("请输入开始值：");
            var start = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入结束值：");
            var end = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient(null, $"127.0.0.1:6379,defaultDatabase={db},poolsize=100,ssl=false,writeBuffer=102400");


            var stw = Stopwatch.StartNew();

            var vals = new Dictionary<string, int>();

            for (int i = start; i < end; i++)
            {
                vals.Add($"{key}_{i}", i);
            }

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 整理完成，耗时{stw.Elapsed.TotalMilliseconds} 毫秒。。。");

            var stw01 = Stopwatch.StartNew();

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis  开始写入。。。");

            rds.StartPipe(p =>
            {
                foreach (var item in vals)
                {
                    p.HSetNx(key, item.Key, item.Value);
                }
            });

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 写入完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");

            stw01.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {end - start} 次写入完成，耗时{stw01.Elapsed.TotalMilliseconds} 毫秒");
        }

        static void HashWriteTest2()
        {
            Console.WriteLine("Hash 写入测试");
            Console.WriteLine("请输入DB：");
            var db = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入KEY：");
            var key = Console.ReadLine(); 

            var task01 = Task.Factory.StartNew(() => HashWriteTest3(db, key, 0, 500000));
            var task02 = Task.Factory.StartNew(() => HashWriteTest3(db, key, 500000, 1000000));
            var task03 = Task.Factory.StartNew(() => HashWriteTest3(db, key, 1000000, 1500000));
            var task04 = Task.Factory.StartNew(() => HashWriteTest3(db, key, 1500000, 2000000));
            var task05 = Task.Factory.StartNew(() => HashWriteTest3(db, key, 2000000, 2500000));
            var task06 = Task.Factory.StartNew(() => HashWriteTest3(db, key, 2500000, 3000000));
            var task07 = Task.Factory.StartNew(() => HashWriteTest3(db, key, 3000000, 3500000));

            while (!task01.IsCompleted || !task02.IsCompleted || !task03.IsCompleted)
            {

            }
        }

        static void HashWriteTest3(int db, string key, int start, int end)
        {
            var id = Task.CurrentId;
            var sb = new StringBuilder();
            sb.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Task: {id} Redis 开始整理。。。");
            var rds = new CSRedis.CSRedisClient(null, $"127.0.0.1:6379,defaultDatabase={db},poolsize=100,ssl=false,writeBuffer=102400");

            var stw = Stopwatch.StartNew();

            var vals = new Dictionary<string, int>();

            for (int i = start; i < end; i++)
            {
                vals.Add($"{key}_{i}", i);
            }

            stw.Stop();
            sb.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 整理完成，耗时{stw.Elapsed.TotalMilliseconds} 毫秒。。。");

            var stw01 = Stopwatch.StartNew();

            sb.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis  开始写入。。。");
             
            foreach (var item in vals)
            {
                rds.HSetNx(key, item.Key, item.Value);
            }

            stw.Stop();
            sb.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 写入完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");

            stw01.Stop();
            sb.AppendLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Task: {id} Redis {end - start} 次写入完成，耗时{stw01.Elapsed.TotalMilliseconds} 毫秒");
            sb.AppendLine();
            sb.AppendLine();
            sb.AppendLine();
            Console.WriteLine(sb);
        }

        static void HashReadTaskTest()
        {
            Console.WriteLine("Hash 读取测试");
            Console.WriteLine("请输入DB：");
            var db = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入KEY：");
            var key = Console.ReadLine();
            Console.WriteLine("请输入开始值：");
            var start = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入结束值：");
            var end = Convert.ToInt32(Console.ReadLine());

            var stw = Stopwatch.StartNew();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始执行。。。");

            var tasks = new List<Task<List<string>>>();
            var task_count = 5;
            var task_total = end - start;
            var task_size = Convert.ToInt32(task_total / task_count);
            task_count = task_total % task_count > 0 ? ++task_count : task_count;

            for (int i = 0; i < task_count; i++)
            {
                tasks.Add(Task.Factory.StartNew<List<string>>(m =>
                {
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 线程{m}开始执行。。。");

                    var task_stw = Stopwatch.StartNew();
                    var task_index = Convert.ToInt32(m);
                    var task_start = task_index * task_size;
                    var task_end = task_index * task_size + task_size;
                    task_end = task_end > task_total ? task_total : task_end;
                    var task_redis = new CSRedis.CSRedisClient(null, $"127.0.0.1:6379,defaultDatabase={db},poolsize=100,ssl=false,writeBuffer=102400");
                    var task_result = new List<string>();
                    for (int j = task_start; j < task_end; j++)
                    {
                        var member = $"{key}_{j}";
                        var a = task_redis.HGet<decimal>(key, member);
                        task_result.Add($"key:{key}      member:{member}      value:{a}");
                    }

                    task_stw.Stop();
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 线程{m} 执行完成,读取数据 {task_result.Count} ，耗时 {task_stw.Elapsed.TotalMilliseconds} ms");

                    return task_result;
                }, i));
            }

            while (!tasks.All(m => m.IsCompleted))
            {

            }

            var result = new List<string>();

            foreach (var task in tasks)
            {
                result.AddRange(task.Result);
            }

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {result.Count} 条数据读取完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");
        }

        static void HashWriteTaskTest()
        {
            Console.WriteLine("Hash 多线程写入测试");
            Console.WriteLine("请输入DB：");
            var db = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入KEY：");
            var key = Console.ReadLine();
            Console.WriteLine("请输入开始值：");
            var start = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("请输入结束值：");
            var end = Convert.ToInt32(Console.ReadLine());

            var stw = Stopwatch.StartNew();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 开始执行。。。");

            var stw1 = Stopwatch.StartNew();

            var vals = new Dictionary<string, int>();

            for (int i = start; i < end; i++)
            {
                vals.Add($"{key}_{i}", i);
            }

            stw1.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 整理完成，耗时{stw1.Elapsed.TotalMilliseconds} 毫秒。。。");

            var tasks = new List<Task>();
            var task_count = 5;
            var task_total = end - start;
            var task_size = Convert.ToInt32(task_total / task_count);
            task_count = task_total % task_count > 0 ? ++task_count : task_count;

            for (int i = 0; i < task_count; i++)
            {
                tasks.Add(Task.Factory.StartNew(m =>
                {
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 线程{m}开始执行。。。");

                    var task_stw = Stopwatch.StartNew();
                    var task_index = Convert.ToInt32(m);
                    var task_start = task_index * task_size;

                    var temp_vals = vals.Skip(task_index * task_size).Take(task_size).ToList();
                    var task_redis = new CSRedis.CSRedisClient(null, $"127.0.0.1:6379,defaultDatabase={db},poolsize=100,ssl=false,writeBuffer=102400");
                    var task_result = new List<string>();

                    foreach (var item in temp_vals)
                    {
                        var isexis = task_redis.HExists(key, item.Key);

                        if (!isexis)
                        {
                            task_redis.HSet(key, item.Key, item.Value);
                        }
                    }

                    task_stw.Stop();
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis 线程{m} 执行完成,写入数据 {temp_vals.Count()} ，耗时 {task_stw.Elapsed.TotalMilliseconds} ms");
                }, i));
            }

            while (!tasks.All(m => m.IsCompleted))
            {

            }

            stw.Stop();
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss:ffff} Redis {vals.Count} 条数据读取完成，耗时 {stw.ElapsedMilliseconds} 毫秒。。。");
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
