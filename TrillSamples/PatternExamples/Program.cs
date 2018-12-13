// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace PatternExamples
{
    internal static class Extensions
    {
        public static void SubscribeToConsole<TP>(this IStreamable<Empty, TP> stream)
        {
            stream.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(x => Console.WriteLine($"Time: {x.StartTime} Payload: {x.Payload}")).Wait();

            Console.WriteLine();
        }
    }

    public sealed class Program
    {
        private static IStreamable<Empty, Payload> source;

        public static void Main(string[] args)
        {
            CreateSourceData();

            // Use functional notation to express patterns in a compact form (without having to utter types)
            FunctionalRegexSamples();

            // Use a static-method-based regex API to explicitly construct an AFA
            ExplicitRegexSamples();

            // Directly specify the augmented finite automaton using arcs
            GeneralAfaSamples();

            Console.WriteLine("Press <ENTER> to exit");
            Console.ReadLine();
        }

        private static void FunctionalRegexSamples()
        {
            #region Regex: AC
            source
                .Detect(p => p.SingleElement(e => e.Field1 == "A")
                              .SingleElement(e => e.Field1 == "C"))
                .SubscribeToConsole();

            source
                .DefinePattern()
                .SingleElement(e => e.Field1 == "A")
                .SingleElement(e => e.Field1 == "C")
                .Detect()
                .SubscribeToConsole();
            #endregion

            #region Regex: A(B*)C and accumulate a sum of Field2 for B*
            source
                .Detect(0, p => p.SingleElement(e => e.Field1 == "A", (ev, d) => 0)
                                 .KleeneStar(r => r.SingleElement(e => e.Field1 == "B", (ev, d) => d + ev.Field2))
                                 .SingleElement(e => e.Field1 == "C"))
                .SubscribeToConsole();

            source
                .DefinePattern()
                .SetRegister(0) // or .SetRegister<int>()
                .SingleElement(e => e.Field1 == "A")
                .KleeneStar(r => r.SingleElement(e => e.Field1 == "B", (ev, d) => d + ev.Field2))
                .SingleElement(e => e.Field1 == "C")
                .Detect()
                .SubscribeToConsole();
            #endregion
        }

        private static void ExplicitRegexSamples()
        {
            // Let's start with stateless regular expressions
            #region Regex 1: AC
            var reg1 =
                ARegex.Concat(
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"),
                    ARegex.SingleElement<Payload>(e => e.Field1 == "C"));
            RunPatternQuery("Regex 1: AC", reg1, source);
            #endregion

            #region Regex 2: A(B*)C
            var reg2 =
                ARegex.Concat(
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"),
                    ARegex.KleeneStar(ARegex.SingleElement<Payload>(e => e.Field1 == "B")),
                    ARegex.SingleElement<Payload>(e => e.Field1 == "C"));
            RunPatternQuery("Regex 2: A(B*)C", reg2, source);
            #endregion

            #region Regex 3: A(B+|C*)A
            var reg3 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"),
                    ARegex.Or
                    (
                        ARegex.KleenePlus(ARegex.SingleElement<Payload>(e => e.Field1 == "B")),
                        ARegex.KleeneStar(ARegex.SingleElement<Payload>(e => e.Field1 == "C"))),
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"));

            RunPatternQuery("Regex 3: A(B+|C*)A", reg3, source);
            #endregion

            #region Regex 4: A(C|epsilon)A
            var reg4 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"),
                    ARegex.Or
                    (
                        ARegex.SingleElement<Payload>(e => e.Field1 == "C"),
                        ARegex.Epsilon<Payload>()),
                    ARegex.SingleElement<Payload>(e => e.Field1 == "A"));

            RunPatternQuery("Regex 4: A(C|epsilon)A", reg4, source);
            #endregion

            // Next step: regular expressions with state (register) accumulation
            #region Regex 5: A(B*)C and accumulate a sum of Field2 for B*
            var reg5 =
                ARegex.Concat(
                    ARegex.SingleElement<Payload, int>(e => e.Field1 == "A", (ev, d) => 0), // start with A
                    ARegex.KleeneStar(
                        ARegex.SingleElement<Payload, int>(
                            e => e.Field1 == "B", (ev, d) => d + ev.Field2)),               // accumulate a sum for B*
                    ARegex.SingleElement<Payload, int>(e => e.Field1 == "C"));              // end with C

            RunPatternQuery("Regex 5: A(B*)C and accumulate a sum of Field2 for B*", reg5, source);
            #endregion

            #region Regex 6: A(B*)C and report list of all contributing events for every match
            var reg6 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload, FList<Payload>>(
                        e => e.Field1 == "A", (ev, list) => new FList<Payload>().FAdd(ev)), // A
                    ARegex.KleeneStar(ARegex.SingleElement<Payload, FList<Payload>>(
                        e => e.Field1 == "B", (ev, list) => list.FAdd(ev))),                // B*
                    ARegex.SingleElement<Payload, FList<Payload>>(
                        e => e.Field1 == "C", (ev, list) => list.FAdd(ev)));                // C

            RunPatternQuery("Regex 6: A(B*)C and report list of all contributing events for every match", reg6, source);
            #endregion

            // Regular expression with time constraints expressed using registers
            #region Regex 7: A is followed by B within 100 time units (AB)
            var reg7 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload, long>(
                        ev => ev.Field1 == "A", (ts, ev, reg) => ts),          // start with A, store timestamp in register
                    ARegex.SingleElement<Payload, long>(
                        (ts, ev, reg) => ev.Field1 == "B" && ts < reg + 100)); // end with B within 100 units of A

            RunPatternQuery("Regex 7: A is followed by B within 100 time units (AB)", reg7, source);
            #endregion

            #region Regex 8: A(.*)C where C is after >= 20 ticks of A

            var reg8 =
                ARegex.Concat
                (
                    ARegex.SingleElement<Payload, long>(
                        e => e.Field1 == "A",
                        (ts, ev, reg) => ts),                                  // A, store timestamp in register
                    ARegex.KleeneStar(ARegex.SingleElement<Payload, long>()),  // (.*)
                    ARegex.SingleElement<Payload, long>(
                        (ts, ev, reg) => ev.Field1 == "C" && ts - reg >= 20)); // C, compare event timestamp with register value

            RunPatternQuery("Regex 8: A(.*)C where C is after >= 20 ticks of A", reg8, source);
            #endregion

            // Regular expressions with state accumulation and local time constraints
            #region Regex 9: Sequence of A's followed by a B within 100 time units [(A*)B] and report the sum of Field2 for A*

            var reg9 =
                ARegex.Concat
                (
                    ARegex.KleeneStar(
                        ARegex.SingleElement<Payload, ValueTuple<int, long>>(
                            ev => ev.Field1 == "A",
                            (ts, ev, reg) => ValueTuple.Create(reg.Item1 + ev.Field2, ts))), // accumulate a sum for A*
                    ARegex.SingleElement<Payload, ValueTuple<int, long>>(
                        (ts, ev, reg) => ev.Field1 == "B" && ts < reg.Item2 + 100));    // end with B within 100 units

            RunPatternQuery("Regex 9: Sequence of A's followed by a B within 100 time units [(A*)B] and report the sum of Field2 for A*", reg9, source);
            #endregion

            #region Regex 10: A(.*)C , C is within 100 time units of A, report (A.Field2 + C.Field2) (skip the .* matches)
            var reg10 =
                ARegex.Concat(
                    ARegex.SingleElement<Payload, ValueTuple<int, long>>(
                        ev => ev.Field1 == "A", (ts, ev, reg) => ValueTuple.Create(ev.Field2, ts)),
                    ARegex.KleeneStar(ARegex.SingleElement<Payload, ValueTuple<int, long>>()),
                    ARegex.SingleElement<Payload, ValueTuple<int, long>>(
                        (ts, ev, reg) => ev.Field1 == "C" && ts < reg.Item2 + 100,
                        (ts, ev, reg) => ValueTuple.Create(reg.Item1 + ev.Field2, reg.Item2)));

            RunPatternQuery("Regex 10: A(.*)C , C is within 100 time units of A, report (A.Field2 + C.Field2) (skip the .* matches)", reg10, source);
            #endregion
        }

        private static void GeneralAfaSamples()
        {
            #region Pattern 1: A followed immediately by B: AB
            var pat1 = new Afa<Payload, Empty>();
            pat1.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A");
            pat1.AddSingleElementArc(1, 2, (ts, ev, reg) => ev.Field1 == "B");
            RunPatternQuery("Pattern 1: A followed immediately by B: AB", pat1, source);
            #endregion

            #region Pattern 2: A followed by B (with events in between): A(.*)B
            var pat2 = new Afa<Payload, Empty>();
            pat2.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A");
            pat2.AddSingleElementArc(1, 1, (ts, ev, reg) => true);
            pat2.AddSingleElementArc(1, 2, (ts, ev, reg) => ev.Field1 == "B");
            RunPatternQuery("Pattern 2: A followed by B (with events in between): A(.*)B", pat2, source);
            #endregion

            #region Pattern 3: A followed by first occurrence of B within 100 time units
            var pat3 = new Afa<Payload, long>();
            pat3.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A", (ts, ev, reg) => ts);
            pat3.AddSingleElementArc(1, 1, (ts, ev, reg) => ev.Field1 != "B" && ts < reg + 100);
            pat3.AddSingleElementArc(1, 2, (ts, ev, reg) => ev.Field1 == "B" && ts < reg + 100);
            RunPatternQuery("Pattern 3: A followed by first occurrence of B within 100 time units", pat3, source);
            #endregion

            #region Pattern 4: A followed by no occurrences of B within 20 time units
            var pat4 = new Afa<Payload, long>();
            pat4.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A", (ts, ev, reg) => ts);
            pat4.AddSingleElementArc(1, 1, (ts, ev, reg) => ev.Field1 != "B" && ts < reg + 20);
            pat4.AddSingleElementArc(1, 2, (ts, ev, reg) => ts >= reg + 20);
            RunPatternQuery("Pattern 4: A followed by no occurrences of B within 20 time units", pat4, source);
            #endregion

            #region Pattern 5: Sequence of A's followed by sequence of B's of same number
            var pat5 = new Afa<Payload, int>();
            pat5.AddSingleElementArc(0, 0, (ts, ev, reg) => ev.Field1 == "A", (ts, ev, reg) => reg + 1);
            pat5.AddSingleElementArc(0, 1, (ts, ev, reg) => ev.Field1 == "A", (ts, ev, reg) => reg + 1);
            pat5.AddSingleElementArc(1, 1, (ts, ev, reg) => ev.Field1 == "B" && reg > 1, (ts, ev, reg) => reg - 1);
            pat5.AddSingleElementArc(1, 2, (ts, ev, reg) => ev.Field1 == "B" && reg == 1, (ts, ev, reg) => reg - 1);
            RunPatternQuery("Pattern 5: Sequence of A's followed by sequence of B's of same number", pat5, source);
            #endregion
        }

        private static void CreateSourceData()
        {
            source = new StreamEvent<Payload>[] {
                StreamEvent.CreatePoint(100, new Payload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new Payload { Field1 = "C", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new Payload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new Payload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new Payload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new Payload { Field1 = "C", Field2 = 7 }),
                StreamEvent.CreatePoint(160, new Payload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000).Cache();
        }

        private static void RunPatternQuery<TP, TR, TA>(string name, Afa<TP, TR, TA> afa, IStreamable<Empty, TP> source)
        {
            Console.WriteLine(name);
            Console.WriteLine("Result: ");
            source.Detect(afa)
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(x => Console.WriteLine("Time: {0} Payload: {1}", x.StartTime, x.Payload)).Wait();
            Console.WriteLine();
        }
        private static void RunPatternQueryGrouped<TP, TR, TK>(string name, Afa<TP, TR> afa, IStreamable<Empty, TP> source, Expression<Func<TP, TK>> groupingKey)
        {
            Console.WriteLine("[Grouped] " + name);
            Console.WriteLine("Result: ");
            source.SetProperty().IsSyncTimeSimultaneityFree(true)
                .GroupApply(groupingKey, gc => gc.Detect(afa), (g, c) => new { Group = g.Key, Payload = c })
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(x => Console.WriteLine("Time: {0} Payload: {1}", x.StartTime, x.Payload)).Wait();
            Console.WriteLine();
        }
    }
}
