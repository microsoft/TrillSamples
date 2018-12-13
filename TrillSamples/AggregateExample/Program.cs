using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reactive.Linq;
using System.Reflection;
using Microsoft.StreamProcessing;

namespace AggregateExample
{
    public class Program
    {
        private static readonly StreamEvent<int>[] values =
        {
                StreamEvent.CreateInterval(1, 10, 0),
                StreamEvent.CreateInterval(2, 10, 1),
                StreamEvent.CreateInterval(3, 10, 2),
                StreamEvent.CreateInterval(4, 10, 3),
                StreamEvent.CreateInterval(5, 10, 4),
                StreamEvent.CreateInterval(6, 10, 5),
                StreamEvent.CreateInterval(7, 10, 6),
                StreamEvent.CreateInterval(8, 10, 7),
                StreamEvent.CreateInterval(9, 10, 8),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
        };

        [DisplayName("CountExample")]
        private static void CountExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report the number of events.
            Console.WriteLine();
            Console.WriteLine("Query: input.Count()");
            var output = input.Count();

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("AverageExample")]
        private static void AverageExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report the average value of payload values.
            Console.WriteLine();
            Console.WriteLine("Query: input.Average(v => v)");
            var output = input.Average(v => v);

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("MinExample")]
        private static void MinExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report the minimum value of payload values.
            Console.WriteLine();
            Console.WriteLine("Query: input.Min(v => v)");
            var output = input.Min(v => v);

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("MaxExample")]
        private static void MaxExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report the maximum value of payload values.
            Console.WriteLine();
            Console.WriteLine("Query: input.Max(v => v)");
            var output = input.Max(v => v);

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("MedianExample")]
        private static void MedianExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report the median value of payload values.
            Console.WriteLine();
            Console.WriteLine("Query: input.Aggregate(w => w.PercentileDiscrete(0.5, v => v))");
            var output = input.Aggregate(w => w.PercentileDiscrete(0.5, v => v));

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("TopKExample")]
        private static void TopKExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report the top 3 values of payload values.
            Console.WriteLine();
            Console.WriteLine("Query: input.TopK(v => v, 3).SelectMany(v => v).Select(v => v.Payload)");
            var output = input.TopK(v => v, 3).SelectMany(v => v).Select(v => v.Payload);

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("CompoundAggregateExample")]
        private static void CompoundAggregateExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report 3 aggregates in one query.
            Console.WriteLine();
            Console.WriteLine("Query: input");
            Console.WriteLine("    .Aggregate(");
            Console.WriteLine("        w => w.Count(),");
            Console.WriteLine("        w => w.Max(v => v),");
            Console.WriteLine("        w => w.Average(v => v),");
            Console.WriteLine("        (count, max, average) => new { Count = count, Max = max, Average = average })");
            var output = input.Aggregate(
                w => w.Count(),
                w => w.Max(v => v),
                w => w.Average(v => v),
                (count, max, average) => new { Count = count, Max = max, Average = average });

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("TumblingWindowCountExample")]
        private static void TumblingWindowCountExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Every 3 time units, report the number of events that were being processed
            // at some point during that period. Report the result at a point in time, at the
            // end of the 3 time units window.
            var duration = 3;
            var offset = 0;
            Console.WriteLine();
            Console.WriteLine("Query: input.TumblingWindowLifetime(3, 0).Count()");
            var output = input.TumblingWindowLifetime(duration, offset).Count();

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("HoppingWindowCountExample")]
        private static void HoppingWindowCountExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report the count of events being processed at some time over a 5 time units window,
            // with the window moving in 3 time units hops. Provide the counts as of the last reported
            // result as of a point in time, reflecting the events processed over the last 5 time units.
            var windowSize = 5;
            var period = 3;
            var offset = 0;
            Console.WriteLine();
            Console.WriteLine("Query: input.HoppingWindowLifetime(5, 3, 0).Count()");
            var output = input.HoppingWindowLifetime(windowSize, period, offset).Count();

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("GroupAggregateExample")]
        private static void GroupAggregateExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Every 5 time units, report the number of events and the sum of payload values for each group key.
            Console.WriteLine();
            Console.WriteLine("Query: input");
            Console.WriteLine("    .TumblingWindowLifetime(5)");
            Console.WriteLine("    .GroupAggregate(");
            Console.WriteLine("        w => w / 3,");
            Console.WriteLine("        w => w.Count(),");
            Console.WriteLine("        w => w.Sum(v => v),");
            Console.WriteLine("        (key, count, sum) => new { Key = key.Key, Count = count, Sum = sum })");

            var output = input
                .TumblingWindowLifetime(5)
                .GroupAggregate(
                    w => w / 3,
                    w => w.Count(),
                    w => w.Sum(v => v),
                    (key, count, sum) => new { Key = key.Key, Count = count, Sum = sum });

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("UDAExample")]
        private static void UDAExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Report the value of a user-defined aggregate function StandardDeviation.
            Console.WriteLine();
            Console.WriteLine("Query: input");
            Console.WriteLine("    .Aggregate(");
            Console.WriteLine("        w => w.StandardDeviation(v => v),");
            Console.WriteLine("        w => w.Count(),");
            Console.WriteLine("        (std, count) => new { StandardDeviation= std, Count = count })");
            var output = input.Aggregate(
                w => w.StandardDeviation(v => v),
                w => w.Count(),
                (std, count) => new { StandardDeviation = std, Count = count });

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        private struct Function
        {
            public readonly MethodInfo Method;
            public readonly string Name;

            public Function(MethodInfo method, string name)
            {
                this.Method = method;
                this.Name = name;
            }
        }

        private static Function[] GetFunctions()
        {
            var functions = new List<Function>();
            foreach (var method in typeof(Program).GetMethods(BindingFlags.Static | BindingFlags.NonPublic))
            {
                var nameAttr = method.GetCustomAttribute<DisplayNameAttribute>();
                if (nameAttr == null)
                {
                    continue;
                }

                functions.Add(new Function(method, nameAttr.DisplayName));
            }

            return functions.ToArray();
        }

        public static void Main(string[] args)
        {
            var demos = GetFunctions();

            while (true)
            {
                Console.WriteLine();
                Console.WriteLine("Pick an action:");
                for (int demo = 0; demo < demos.Length; demo++)
                {
                    Console.WriteLine($"{demo, 4} - {demos[demo].Name}");
                }

                Console.WriteLine("Exit - Exit from Demo.");
                var response = Console.ReadLine().Trim();
                if (string.Equals(response, "exit", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(response, "e", StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }

                int demoToRun;
                if (!int.TryParse(response, NumberStyles.Integer, CultureInfo.InvariantCulture, out demoToRun))
                {
                    demoToRun = -1;
                }

                if (demoToRun >= 0 && demoToRun < demos.Length)
                {
                    Console.WriteLine();
                    Console.WriteLine(demos[demoToRun].Name);
                    demos[demoToRun].Method.Invoke(null, null);
                }
                else
                {
                    Console.WriteLine("Unknown Query Demo");
                }
            }
        }
    }
}