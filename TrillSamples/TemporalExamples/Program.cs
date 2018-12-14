// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Reactive.Linq;
using System.Reflection;
using Microsoft.StreamProcessing;

namespace TemporalExamples
{
    public sealed class Program
    {
        private static readonly StreamEvent<string>[] values =
        {
                StreamEvent.CreateInterval(11, 15, "a"),
                StreamEvent.CreateInterval(12, 14, "b"),
                StreamEvent.CreateInterval(21, 26, "c"),
                StreamEvent.CreateInterval(25, 31, "a"),
                StreamEvent.CreateInterval(26, 28, "b"),
                StreamEvent.CreateInterval(31, 35, "c"),
                StreamEvent.CreateInterval(33, 34, "a"),
                StreamEvent.CreateInterval(41, 45, "b"),
                StreamEvent.CreateInterval(42, 48, "c"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
        };

        private static readonly StreamEvent<string>[] values2 =
        {
                StreamEvent.CreateInterval(26, 34, "a"),
                StreamEvent.CreateInterval(27, 43, "b"),
                StreamEvent.CreateInterval(33, 45, "c"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
        };

        [DisplayName("ExtendLifetimeExample")]
        private static void ExtendLifetimeExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Extend all event lifetimes by 3.
            Console.WriteLine();
            Console.WriteLine("Query: input.ExtendLifetime(3)");
            var output = input.ExtendLifetime(3);

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("UnaryClipExample")]
        private static void UnaryClipExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Truncate all event lifetimes by a maximum lifetime length of 3.
            Console.WriteLine();
            Console.WriteLine("Query: input.ClipEventDuration(3)");
            var output = input.ClipEventDuration(3);

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("BinaryClipExample")]
        private static void BinaryClipExample()
        {
            var input1 = values.ToObservable().ToStreamable();
            Console.WriteLine("Input1 =");
            input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            var input2 = values2.ToObservable().ToStreamable();
            Console.WriteLine();
            Console.WriteLine("Input2 =");
            input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Truncate the lifetime of each event in Input1 by the first event in Input2
            // which has the same key and occurs later than the event in Input1.
            Console.WriteLine();
            Console.WriteLine("Query: input1.ClipEventDuration(input2, e => e, e => e)");
            var output = input1.ClipEventDuration(input2, e => e, e => e);

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("SessionTimeoutExample")]
        private static void SessionTimeoutExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Extend all event lifetimes as long as new events arrive within a period of 6.
            Console.WriteLine();
            Console.WriteLine("Query: input.SessionTimeoutWindow(6)");
            var output = input.SessionTimeoutWindow(6);

            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("PointAtEndExample")]
        private static void PointAtEndExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Convert all interval events into point events at end.
            Console.WriteLine();
            Console.WriteLine("Query: input.PointAtEnd()");
            var output = input.PointAtEnd();

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
