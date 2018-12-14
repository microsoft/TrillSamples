// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reflection;
using Microsoft.StreamProcessing;

namespace StatelessExamples
{
    public sealed class Program
    {
        public struct Point
        {
            public int x;
            public int y;

            public override string ToString() => $" {{x:{this.x}, y:{this.y}}}";
        }

        public struct Point1D
        {
            public int x;

            public override string ToString() => $" {{x:{this.x}}}";
        }

        public struct Point3D
        {
            public int x;
            public int y;
            public int z;

            public override string ToString() => $" {{x:{this.x}, y:{this.y}, z:{this.z}}}";
        }

        public struct Point3DRefTypes
        {
            public string x;
            public string y;
            public string z;

            public override string ToString() => $" {{x:{this.x}, y:{this.y}, z:{this.z}}}";
        }

        private static readonly StreamEvent<Point>[] values =
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

        [DisplayName("WhereExampleSingleField")]
        private static void WhereExampleSingleField()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Apply a filter on one field.
            Console.WriteLine();
            Console.WriteLine("Query: input.Where(p => p.x > 5)");
            var output = input.Where(p => p.x > 5);

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("WhereExampleCorrelatedFields")]
        private static void WhereExampleCorrelatedFields()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Apply a filter on two correlated fields.
            Console.WriteLine();
            Console.WriteLine("Query: input.Where(p => p.x + 5 > p.y)");
            var output = input.Where(p => p.x + 5 > p.y);

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("SelectExampleFieldExtraction")]
        private static void SelectExampleFieldExtraction()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Map each Point object to a Point1D object by extracting the field x.
            Console.WriteLine();
            Console.WriteLine("Query: input.Select(p => new Point1D { x = p.x })");
            var output = input.Select(p => new Point1D { x = p.x });

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("SelectExampleAnonymousType")]
        private static void SelectExampleAnonymousType()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Map each Point object to an anonymous type object.
            Console.WriteLine();
            Console.WriteLine("Query: input.Select(p => new { Distance = sqrt(p.x * p.x + p.y * p.y), Text = \"Point #\" + p.x })");
            var output = input.Select(
                p => new { Distance = Math.Round(Math.Sqrt((p.x * p.x) + (p.y * p.y)), 2), Text = "Point #" + p.x });

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("SelectExampleAutomaticFieldExtraction")]
        private static void SelectExampleAutomaticFieldExtraction()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Map each Point object to a Point1D object (same as SelectExample1) by
            // automatically extracting fields defined in both types.
            Console.WriteLine();
            Console.WriteLine("Query: input.Select(() => new Point1D())");
            var output = input.Select(() => new Point1D());

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("SelectExampleNewFields")]
        private static void SelectExampleNewFields()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.WriteLine();
            Console.WriteLine("Query: input.Select(() => new Point3D(), p => p.z, p => p.x + p.y)");
            // Map each Point object to a Point3D object by extracting existing fields and adding a new field z.
            var output = input.Select(() => new Point3D(), p => p.z, p => p.x + p.y);

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("SelectExampleColumnAdjustment")]
        private static void SelectExampleColumnAdjustment()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            Console.WriteLine();
            Console.WriteLine("Query: input.Select(() => new Point3D(), new Dictionary{{\"y\", p => p.y * 2 }, {\"z\", p => p.x + p.y }})");
            // Map each Point object to a Point3D object by extracting the field x, updating the field y, and adding a new field z.
            var output = input.Select(
                () => new Point3D(),
                new Dictionary<string, Expression<Func<Point, object>>>
                {
                    { "y", p => p.y * 2 },
                    { "z", p => p.x + p.y }
                });

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("SelectManyExample")]
        private static void SelectManyExample()
        {
            var input = values.ToObservable().ToStreamable();
            Console.WriteLine("Input =");
            input.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Map each Point object to three Point objects.
            Console.WriteLine();
            Console.WriteLine("Query: input.SelectMany(p => Enumerable.Repeat(p, 3))");
            var output = input.SelectMany(p => Enumerable.Repeat(p, 3));

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
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
