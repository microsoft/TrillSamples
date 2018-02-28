using Microsoft.StreamProcessing;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reactive.Linq;
using System.Reflection;

namespace PatternExamples
{
    class Program
    {
        static IStreamable<Empty, Payload> source1;
        static IStreamable<Empty, Payload> source2;

        static void CreateData()
        {
            source1 = new StreamEvent<Payload>[] {
                StreamEvent.CreatePoint(100, new Payload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new Payload { Field1 = "C", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new Payload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new Payload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new Payload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new Payload { Field1 = "C", Field2 = 7 }),
                StreamEvent.CreatePoint(160, new Payload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable(OnCompletedPolicy.EndOfStream()).AlterEventDuration(1000).Cache();

            source2 = new StreamEvent<Payload>[] {
                StreamEvent.CreatePoint(100, new Payload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(110, new Payload { Field1 = "C", Field2 = 2 }),
                StreamEvent.CreatePoint(120, new Payload { Field1 = "A", Field2 = 2 }),
                StreamEvent.CreatePoint(130, new Payload { Field1 = "B", Field2 = 2 }),
                StreamEvent.CreatePoint(140, new Payload { Field1 = "B", Field2 = 2 }),
                StreamEvent.CreatePoint(150, new Payload { Field1 = "C", Field2 = 1 }),
                StreamEvent.CreatePoint(160, new Payload { Field1 = "B", Field2 = 1 }),
            }.ToObservable().ToStreamable(OnCompletedPolicy.EndOfStream()).AlterEventDuration(1000).Cache();
        }

        [DisplayName("FunctionalRegexExample")]
        static void FunctionalRegexExamples()
        {
            // Use functional notation to express patterns in a compact form (without having to utter types)
            #region Regex: AC
            Console.WriteLine("Regex 1: AC");
            Console.WriteLine("Result: ");
            source1
                .Detect(p => p.SingleElement(e => e.Field1 == "A")
                              .SingleElement(e => e.Field1 == "C"))
                .SubscribeToConsole();
            Console.WriteLine();

            Console.WriteLine("Regex 2: AC");
            Console.WriteLine("Result: ");
            source1
                .DefinePattern()
                .SingleElement(e => e.Field1 == "A")
                .SingleElement(e => e.Field1 == "C")
                .Detect()
                .SubscribeToConsole();
            Console.WriteLine();
            #endregion

            #region Regex: A(B*)C and accumulate a sum of Field2 for B*
            Console.WriteLine("Regex 3: A(B*)C and accumulate a sum of Field2 for B*");
            Console.WriteLine("Result: ");
            source1
                .Detect(0, p => p.SingleElement(e => e.Field1 == "A", (ev, d) => 0)
                                 .KleeneStar(r => r.SingleElement(e => e.Field1 == "B", (ev, d) => d + ev.Field2))
                                 .SingleElement(e => e.Field1 == "C"))
                .SubscribeToConsole();
            Console.WriteLine();

            Console.WriteLine("Regex 3: A(B*)C and accumulate a sum of Field2 for B*");
            Console.WriteLine("Result: ");
            source1
                .DefinePattern()
                .SetRegister(0) // or .SetRegister<int>()
                .SingleElement(e => e.Field1 == "A")
                .KleeneStar(r => r.SingleElement(e => e.Field1 == "B", (ev, d) => d + ev.Field2))
                .SingleElement(e => e.Field1 == "C")
                .Detect()
                .SubscribeToConsole();
            Console.WriteLine();
            #endregion
        }

        [DisplayName("ExplicitRegexExample")]
        static void ExplicitRegexExamples()
        {
            // Use a static-method-based regex API to explicitly construct an AFA
            // Let's start with stateless regular expressions
            #region Regex 1: AC
            var reg1 =
                ARegex.Concat(
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"),
                    ARegex.SingleElement<Payload>(e => e.Field1 == "C")
                );
            RunPatternQuery("Regex 1: AC", reg1, source1);
            #endregion

            #region Regex 2: A(B*)C
            var reg2 =
                ARegex.Concat(
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"),
                    ARegex.KleeneStar(ARegex.SingleElement<Payload>(e => e.Field1 == "B")),
                    ARegex.SingleElement<Payload>(e => e.Field1 == "C")
                );
            RunPatternQuery("Regex 2: A(B*)C", reg2, source1);
            #endregion

            #region Regex 3: A(B+|C*)D
            var reg3 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"),
                    ARegex.Or
                    (
                        ARegex.KleenePlus(ARegex.SingleElement<Payload>(e => e.Field1 == "B")),
                        ARegex.KleeneStar(ARegex.SingleElement<Payload>(e => e.Field1 == "C"))
                    ),
                    ARegex.SingleElement<Payload>(e => e.Field1 == "D")
                );
            RunPatternQuery("Regex 3: A(B+|C*)D", reg3, source1);
            #endregion

            #region Regex 4: A(C|epsilon)A
            var reg4 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"),
                    ARegex.Or
                    (
                        ARegex.SingleElement<Payload>(e => e.Field1 == "C"),
                        ARegex.Epsilon<Payload>()
                    ),
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A")
                );
            RunPatternQuery("Regex 4: A(C|epsilon)A", reg4, source1);
            #endregion

            // Next step: regular expressions with state (register) accumulation
            #region Regex 5: A(B*)C and accumulate a sum of Field2 for B*
            var reg5 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload, int>(e => e.Field1 == "A", (ev, d) => 0), // start with A
                    ARegex.KleeneStar(ARegex.SingleElement<Payload, int>(e => e.Field1 == "B", (ev, d) => d + ev.Field2)), // accumulate a sum for B*
                    ARegex.SingleElement<Payload, int>(e => e.Field1 == "C") // end with C
                );
            RunPatternQuery("Regex 5: A(B*)C and accumulate a sum of Field2 for B*", reg5, source1);
            #endregion

            #region Regex 6: A(B*)C and report list of all contributing events for every match
            var reg6 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload, FList<Payload>>(e => e.Field1 == "A", (ev, list) => new FList<Payload>().FAdd(ev)), // A
                    ARegex.KleeneStar(ARegex.SingleElement<Payload, FList<Payload>>(e => e.Field1 == "B", (ev, list) => list.FAdd(ev))), // B*
                    ARegex.SingleElement<Payload, FList<Payload>>(e => e.Field1 == "C", (ev, list) => list.FAdd(ev)) // C
                );
            RunPatternQuery("Regex 6: A(B*)C and report list of all contributing events for every match", reg6, source1);
            #endregion


            // Regular expression with time constraints expressed using registers
            #region Regex 7: A is followed by B within 100 time units (AB)
            var reg7 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload, long>(ev => ev.Field1 == "A", (ts, ev, reg) => ts), // start with A, store timestamp in register
                    ARegex.SingleElement<Payload, long>((ts, ev, reg) => ev.Field1 == "B" && ts < reg + 100) // end with B within 100 units of A
                );
            RunPatternQuery("Regex 7: A is followed by B within 100 time units (AB)", reg7, source1);
            #endregion

            #region Regex 8: ABC where C is within 100 ticks of A
            var reg8 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload, long>(e => e.Field1 == "A", (ts, ev, reg) => ts), // A, store timestamp in register
                    ARegex.SingleElement<Payload, long>(e => e.Field1 == "B"), // B
                    ARegex.SingleElement<Payload, long>((ts, ev, reg) => ev.Field1 == "C" && ts - reg < 100) // C, compare event timestamp with register value
                );
            RunPatternQuery("Regex 8: ABC where C is within 100 ticks of A", reg8, source1);
            #endregion


            // Regular expressions with state accumulation and local time constraints
            #region Regex 9: Sequence of A's followed by a B within 100 time units [(A*)B] and report the sum of Field2 for A*
            var reg9 =
                ARegex.Concat
                (
                    ARegex.KleeneStar(ARegex.SingleElement<Payload, ValueTuple<int, long>>(ev => ev.Field1 == "A", (ts, ev, reg) => new ValueTuple<int, long> { Item1 = reg.Item1 + ev.Field2, Item2 = ts })), // accumulate a sum for A*
                    ARegex.SingleElement<Payload, ValueTuple<int, long>>((ts, ev, reg) => ev.Field1 == "B" && ts < reg.Item2 + 100) // end with B within 100 units
                );
            RunPatternQuery("Regex 9: Sequence of A's followed by a B within 100 time units [(A*)B] and report the sum of Field2 for A*", reg9, source1);
            #endregion

            #region Regex 10: A(.*)C , C is within 100 time units of A, report (A.Field2 + C.Field2) (skip the .* matches)
            var reg10 =
                ARegex.Concat(
                    ARegex.SingleElement<Payload, ValueTuple<int, long>>(ev => ev.Field1 == "A", (ts, ev, reg) => new ValueTuple<int, long> { Item1 = ev.Field2, Item2 = ts }),
                    ARegex.KleeneStar(ARegex.SingleElement<Payload, ValueTuple<int, long>>()),
                    ARegex.SingleElement<Payload, ValueTuple<int, long>>((ts, ev, reg) => ev.Field1 == "C" && ts < reg.Item2 + 100, (ts, ev, reg) => new ValueTuple<int, long> { Item1 = reg.Item1 + ev.Field2, Item2 = reg.Item2 })
                );
            RunPatternQuery("Regex 10: A(.*)C , C is within 100 time units of A, report (A.Field2 + C.Field2) (skip the .* matches)", reg10, source1);
            #endregion
        }

        [DisplayName("GeneralAfaExample")]
        static void GeneralAfaExamples()
        {
            // Directly specify the augmented finite automaton using arcs
            #region Pattern 1: A followed immediately by B: AB
            Afa<Payload, Empty> pat1 = new Afa<Payload, Empty>();
            pat1.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A");
            pat1.AddSingleElementArc(1, 2, (ts, ev, reg) => ev.Field1 == "B");
            RunPatternQuery("Pattern 1: A followed immediately by B: AB", pat1, source1);
            #endregion

            #region Pattern 2: A followed by B (with events in between): A(.*)B
            Afa<Payload, Empty> pat2 = new Afa<Payload, Empty>();
            pat2.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A");
            pat2.AddSingleElementArc(1, 1, (ts, ev, reg) => true);
            pat2.AddSingleElementArc(1, 2, (ts, ev, reg) => ev.Field1 == "B");
            RunPatternQuery("Pattern 2: A followed by B (with events in between): A(.*)B", pat2, source1);
            #endregion

            #region Pattern 3: A followed by first occurrence of B within 100 time units
            Afa<Payload, long> pat3 = new Afa<Payload, long>();
            pat3.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A", (ts, ev, reg) => ts);
            pat3.AddSingleElementArc(1, 1, (ts, ev, reg) => ev.Field1 != "B" && ts < reg + 100);
            pat3.AddSingleElementArc(1, 2, (ts, ev, reg) => ev.Field1 == "B" && ts < reg + 100);
            RunPatternQuery("Pattern 3: A followed by first occurrence of B within 100 time units", pat3, source1);
            #endregion

            #region Pattern 4: A followed by no occurrences B within 100 time units
            Afa<Payload, long> pat4 = new Afa<Payload, long>();
            pat4.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A", (ts, ev, reg) => ts);
            pat4.AddSingleElementArc(1, 1, (ts, ev, reg) => ev.Field1 != "B" && ts < reg + 100);
            pat4.AddSingleElementArc(1, 2, (ts, ev, reg) => ts >= reg + 100);
            RunPatternQuery("Pattern 4: A followed by no occurrences B within 100 time units", pat4, source1);
            #endregion

            #region Pattern 5: Sequence of A's followed by sequence of B's of same number
            Afa<Payload, int> pat5 = new Afa<Payload, int>();
            pat5.AddSingleElementArc(0, 0, (ts, ev, reg) => ev.Field1 == "A", (ts, ev, reg) => reg + 1);
            pat5.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A", (ts, ev, reg) => reg + 1);
            pat5.AddSingleElementArc(1, 1, (ts, ev, reg) => ev.Field1 == "B" && reg > 1, (ts, ev, reg) => reg - 1);
            pat5.AddSingleElementArc(1, 2, (ts, ev, reg) => ev.Field1 == "B" && reg == 1, (ts, ev, reg) => reg - 1);
            RunPatternQuery("Pattern 5: Sequence of A's followed by sequence of B's of same number", pat5, source1);
            #endregion
        }

        static void RunPatternQuery<TP, TR, TA>(string name, Afa<TP, TR, TA> afa, IStreamable<Empty, TP> source)
        {
            Console.WriteLine(name);
            Console.WriteLine("Result: ");
            source.Detect(afa).ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(x => Console.WriteLine("Time: {0} Payload: {1}", x.StartTime, x.Payload)).Wait();
            Console.WriteLine();
        }

        private struct Function
        {
            public readonly MethodInfo Method;

            public readonly string Name;

            public Function(MethodInfo method, string name)
            {
                Method = method;
                Name = name;
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

        static void Main(string[] args)
        {
            var demos = GetFunctions();

            CreateData();

            while (true)
            {
                Console.WriteLine();
                Console.WriteLine("Pick an action:");
                for (int demo = 0; demo < demos.Length; demo++)
                {
                    Console.WriteLine("{0,4} - {1}", demo, demos[demo].Name);
                }

                Console.WriteLine("Exit - Exit from Demo.");
                var response = Console.ReadLine().Trim();
                if (string.Equals(response, "exit", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(response, "e", StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }

                int demoToRun;
                if (int.TryParse(response, NumberStyles.Integer, CultureInfo.InvariantCulture, out demoToRun) == false)
                {
                    demoToRun = -1;
                }

                if (0 <= demoToRun && demoToRun < demos.Length)
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

    internal static class Extensions
    {
        public static void SubscribeToConsole<TP>(this IStreamable<Empty, TP> stream)
        {
            stream.ToStreamEventObservable()
                .Where(e => e.IsData).ForEachAsync(x => Console.WriteLine("Time: {0} Payload: {1}", x.StartTime, x.Payload)).Wait();

            Console.WriteLine();
        }
    }
}
