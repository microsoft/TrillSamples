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

namespace IngressEgressExamples
{

    public sealed class Program
    {
        private struct Point
        {
            public int x;
            public int y;

            public override string ToString() => $" {{x:{this.x}, y:{this.y}}}";
        };

        [DisplayName("ArrayBasedIngressExample")]
        private static void ArrayBasedIngressExample()
        {
            // Create the first array segment.
            StreamEvent<Point>[] values1 =
            {
                StreamEvent.CreateInterval(1, 10, new Point { x = 1, y = 2 }),
                StreamEvent.CreateInterval(2, 10, new Point { x = 2, y = 4 }),
                StreamEvent.CreateInterval(3, 10, new Point { x = 3, y = 6 }),
                StreamEvent.CreateInterval(4, 10, new Point { x = 4, y = 8 }),
                StreamEvent.CreateInterval(5, 10, new Point { x = 5, y = 10 })
            };

            // Create the second array segment.
            StreamEvent<Point>[] values2 =
            {
                StreamEvent.CreateInterval(6, 10, new Point { x = 6, y = 12 }),
                StreamEvent.CreateInterval(7, 10, new Point { x = 7, y = 14 }),
                StreamEvent.CreateInterval(8, 10, new Point { x = 8, y = 16 }),
                StreamEvent.CreateInterval(9, 10, new Point { x = 9, y = 18 }),
            };

            var segment1 = new ArraySegment<StreamEvent<Point>>(values1);
            var segment2 = new ArraySegment<StreamEvent<Point>>(values2);
            var segments = new ArraySegment<StreamEvent<Point>>[] { segment1, segment2 };

            // Array-based ingress.
            var input = segments.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.ReadLine();
        }

        [DisplayName("ArrayBasedEgressExample")]
        private static void ArrayBasedEgressExample()
        {
            StreamEvent<Point>[] values =
            {
                StreamEvent.CreateInterval(1, 10, new Point { x = 1, y = 2 }),
                StreamEvent.CreateInterval(2, 10, new Point { x = 2, y = 4 }),
                StreamEvent.CreateInterval(3, 10, new Point { x = 3, y = 6 }),
                StreamEvent.CreateInterval(4, 10, new Point { x = 4, y = 8 }),
                StreamEvent.CreateInterval(5, 10, new Point { x = 5, y = 10 }),
                StreamEvent.CreateInterval(6, 10, new Point { x = 6, y = 12 }),
                StreamEvent.CreateInterval(7, 10, new Point { x = 7, y = 14 }),
                StreamEvent.CreateInterval(8, 10, new Point { x = 8, y = 16 }),
                StreamEvent.CreateInterval(9, 10, new Point { x = 9, y = 18 }),
                StreamEvent.CreatePunctuation<Point>(StreamEvent.InfinitySyncTime)
            };

            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Array-based egress.
            Console.WriteLine();
            Console.WriteLine("Output =");
            input.ToStreamEventArrayObservable().ForEachAsync(
                e => e.ToObservable().ForEachAsync(w => Console.WriteLine(w))).Wait();

            Console.ReadLine();
        }

        [DisplayName("AtemporalExample")]
        private static void AtemporalExample()
        {
            var points = new Point[]
            {
                new Point { x = 1, y = 2 },
                new Point { x = 2, y = 4 },
                new Point { x = 3, y = 6 },
                new Point { x = 4, y = 8 },
                new Point { x = 5, y = 10 },
                new Point { x = 6, y = 12 },
                new Point { x = 7, y = 14 },
                new Point { x = 8, y = 16 },
                new Point { x = 9, y = 18 }
            };

            // Atemporal ingress operator.
            var input = points.ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(5));
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            var output = input;

            // Atemporal egress operator.
            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToAtemporalObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

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