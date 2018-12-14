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

namespace JoinExamples
{

    public sealed class Program
    {
        public struct Session
        {
            public int id;
            public int type;

            public override string ToString() => $" {{id:{this.id}, type:{this.type}}}";
        }

        private static readonly StreamEvent<Session>[] sessions1 =
        {
            StreamEvent.CreateInterval(1, 10, new Session { id = 1, type = 11 }),
            StreamEvent.CreateInterval(1, 10, new Session { id = 2, type = 12 }),
            StreamEvent.CreateInterval(3, 10, new Session { id = 3, type = 13 }),
            StreamEvent.CreateInterval(3, 10, new Session { id = 1, type = 14 }),
            StreamEvent.CreateInterval(5, 10, new Session { id = 2, type = 15 }),
            StreamEvent.CreateInterval(5, 10, new Session { id = 3, type = 11 }),
            StreamEvent.CreateInterval(7, 10, new Session { id = 1, type = 12 }),
            StreamEvent.CreateInterval(7, 10, new Session { id = 2, type = 13 }),
            StreamEvent.CreateInterval(9, 10, new Session { id = 3, type = 14 }),
            StreamEvent.CreatePunctuation<Session>(StreamEvent.InfinitySyncTime)
        };

        private static readonly StreamEvent<Session>[] sessions2 =
        {
            StreamEvent.CreateInterval(2, 10, new Session { id = 1, type = 21 }),
            StreamEvent.CreateInterval(4, 10, new Session { id = 2, type = 22 }),
            StreamEvent.CreateInterval(6, 10, new Session { id = 3, type = 23 }),
            StreamEvent.CreateInterval(8, 10, new Session { id = 4, type = 24 }),
            StreamEvent.CreatePunctuation<Session>(StreamEvent.InfinitySyncTime)
        };

        [DisplayName("CrossJoinExample")]
        private static void CrossJoinExample()
        {
            var input1 = sessions1.ToObservable().ToStreamable();
            Console.WriteLine("Input1 =");
            input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            var input2 = sessions2.ToObservable().ToStreamable();
            Console.WriteLine("Input2 =");
            input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Corss join and generate anonymous type objects.
            Console.WriteLine();
            Console.WriteLine("Query:");
            Console.WriteLine("    input1.Join(");
            Console.WriteLine("        input2,");
            Console.WriteLine("        (left, right) => new { ID1 = left.id, Type1 = left.type, ID2 = right.id, Type2 = right.type })");
            var output = input1.Join(
                input2,
                (left, right) => new { ID1 = left.id, Type1 = left.type, ID2 = right.id, Type2 = right.type });

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("EquiJoinExample")]
        private static void EquiJoinExample()
        {
            var input1 = sessions1.ToObservable().ToStreamable();
            Console.WriteLine("Input1 =");
            input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            var input2 = sessions2.ToObservable().ToStreamable();
            Console.WriteLine("Input2 =");
            input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Equi join on the id field and generate anonymous type objects.
            Console.WriteLine();
            Console.WriteLine("Query:");
            Console.WriteLine("    input1.Join(");
            Console.WriteLine("        input2,");
            Console.WriteLine("        w => w.id,");
            Console.WriteLine("        w => w.id,");
            Console.WriteLine("        (left, right) => new { ID = left.id, Type1 = left.type, Type2 = right.type })");
            var output = input1.Join(
                input2,
                w => w.id,
                w => w.id,
                (left, right) => new { ID = left.id, Type1 = left.type, Type2 = right.type });

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("AntiJoinExample")]
        private static void AntiJoinExample()
        {
            var input1 = sessions1.ToObservable().ToStreamable();
            Console.WriteLine("Input1 =");
            input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            var input2 = sessions2.ToObservable().ToStreamable();
            Console.WriteLine("Input2 =");
            input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Left anti join.
            Console.WriteLine();
            Console.WriteLine("Query: input.WhereNotExists(input2, w => w.id, w => w.id)");
            var output = input1.WhereNotExists(input2, w => w.id, w => w.id);

            Console.WriteLine();
            Console.WriteLine("Output =");
            output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
        }

        [DisplayName("OuterJoinExample")]
        private static void OuterJoinExample()
        {
            var input1 = sessions1.ToObservable().ToStreamable();
            Console.WriteLine("Input1 =");
            input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            var input2 = sessions2.ToObservable().ToStreamable();
            Console.WriteLine("Input2 =");
            input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

            // Left outer join.
            Console.WriteLine();
            Console.WriteLine("Query:");
            Console.WriteLine("    input.LeftOuterJoin(");
            Console.WriteLine("        input1,");
            Console.WriteLine("        w => w.id,");
            Console.WriteLine("        w => w.id,");
            Console.WriteLine("        w => new { ID = w.id, Type1 = w.type, Type2 = 0 }, ");
            Console.WriteLine("        (left, right) => new { ID = left.id, Type1 = left.type, Type2 = right.type })");
            var output = input2.LeftOuterJoin(
                input1,
                w => w.id,
                w => w.id,
                w => new { ID = w.id, Type1 = w.type, Type2 = 0 },
                (left, right) => new { ID = left.id, Type1 = left.type, Type2 = right.type });

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
