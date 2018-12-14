// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reactive.Linq;
using System.Reflection;
using Microsoft.StreamProcessing;

namespace FunctionExamples
{
    public static class Program
    {
        private static readonly StreamEvent<int>[] values =
            {
                StreamEvent.CreateInterval(1, 10, 1),
                StreamEvent.CreateInterval(2, 10, 2),
                StreamEvent.CreateInterval(3, 10, 3),
                StreamEvent.CreateInterval(4, 10, 4),
                StreamEvent.CreateInterval(5, 10, 5),
                StreamEvent.CreateInterval(6, 10, 6),
                StreamEvent.CreateInterval(7, 10, 7),
                StreamEvent.CreateInterval(8, 10, 8),
                StreamEvent.CreateInterval(9, 10, 9),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

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

        [DisplayName("Where")]
        private static void WhereFunc()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.WriteLine();
            Console.WriteLine("Query: input.Where(r => r > 5)");
            var output = input.Where(r => r > 5);

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("Select")]
        private static void SelectFunc()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.WriteLine();
            Console.WriteLine("Query: input.Select(r => new { Original = r, Squared = r * r, Text = \"Hello #\" + r })");
            var output = input.Select(r => new { Original = r, Squared = r * r, Text = "Hello #" + r });

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("AlterEventLifetime")]
        private static void AlterLifetimeFunc()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.WriteLine();
            Console.WriteLine("Query: input.AlterEventLifetime(oldStart => oldStart + 5, 2)");
            var output = input.AlterEventLifetime(oldStart => oldStart + 5, 2);

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("SelectMany")]
        private static void SelectManyFunc()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.WriteLine();
            Console.WriteLine("Query: input.SelectMany(r => Enumerable.Repeat(r, 5))");
            var output = input.SelectMany(r => Enumerable.Repeat(r, 5));

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("Count")]
        private static void CountFunc()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.WriteLine();
            Console.WriteLine("Query: input.Count()");
            var output = input.Count();

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
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

                if (!int.TryParse(response, NumberStyles.Integer, CultureInfo.InvariantCulture, out int demoToRun))
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
