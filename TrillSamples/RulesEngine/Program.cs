// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.StreamProcessing;

namespace RulesEngine
{
    internal sealed class TestObservable<T> : IObservable<T>
    {
        private readonly IEnumerable<T> events;

        public TestObservable(IEnumerable<T> events) => this.events = events;

        public IDisposable Subscribe(IObserver<T> observer)
        {
            foreach (var t in this.events) observer.OnNext(t);
            observer.OnCompleted();
            return null;
        }
    }

    internal sealed class TestObserver<T> : IObserver<T>
    {
        public TestObserver(IObservable<T> events) => events.Subscribe(this);

        public void OnCompleted()
        {
            Console.WriteLine("Completed!");
            Console.ReadLine();
        }

        public void OnError(Exception error) => Console.WriteLine(error.ToString());

        public void OnNext(T value) => Console.WriteLine(value.ToString());
    }

    public static class Class1
    {
        internal static IStreamable<PartitionKey<TRulesKey>, ValueTuple<string, TOutput>> RulesEngine<TRulesKey, TInput, TData, TOutput>(
            IStreamable<Empty, TInput> streamable,
            Func<TInput, IEnumerable<TRulesKey>> ruleAssignment,
            Func<TRulesKey, long, long> startEdgeFunction,
            Func<TRulesKey, long, long> durationFunction,
            Func<TInput, TData> dataSelector,
            Func<TRulesKey, SortedMultiSet<TData>, IEnumerable<ValueTuple<string, TOutput>>> aggregator,
            long punctuationLag)
        {
            return streamable
                .SelectMany((input) => ruleAssignment(input).Select(key => ValueTuple.Create(key, input)))
                .Partition(tuple => tuple.Item1, punctuationLag)
                .AlterEventLifetime((p, s) => startEdgeFunction(p, s), (p, s, e) => durationFunction(p, s))
                .Select(o => ValueTuple.Create(o.Item1, dataSelector(o.Item2)))
                .GroupAggregate(o => o.Item1, w => new RulesAggregate<TRulesKey, TData, TOutput>(aggregator), (g, i) => i)
                .SelectMany(o => o);
        }

        private static long SnapToLeftBoundary(this long value, long period)
            => period <= 1 ? value : value - (value % period);

        private static long HoppingWindowStartTime(long startTime, long period)
        {
            if ((startTime > StreamEvent.MaxSyncTime - period) && // Cheap check which fails majority of the time
                (startTime > (StreamEvent.MaxSyncTime / period) * period)) // More expensive precise check
            {
                throw new Exception("Window start out of range");
            }
            return SnapToLeftBoundary(startTime + period - 1, period);
        }

        private static IEnumerable<StreamEvent<ValueTuple<string, int>>> GetStreamEvents()
        {
            foreach (var i in Enumerable.Range(0, 100000))
            {
                yield return StreamEvent.CreatePoint((i / 1000) * 1000, ValueTuple.Create("Device" + (i % 10), i / 100 * 100));
            }
        }

        private static IObservable<StreamEvent<ValueTuple<string, int>>> GetObservable()
            => new TestObservable<StreamEvent<ValueTuple<string, int>>>(GetStreamEvents());

        private static IEnumerable<string> AssignRules(ValueTuple<string, int> tuple)
        {
            if (tuple.Item1 == "Device5") return new[] { "A" };
            if (tuple.Item1 == "Device8") return new[] { "B" };
            if (tuple.Item1 == "Device2") return new[] { "F" };
            return Enumerable.Empty<string>();
        }

        private static long AssignStartEdge(string ruleKey, long oldStart)
        {
            switch (ruleKey)
            {
                // For rule A, assume a tumbling window of size 1000
                case "A": return HoppingWindowStartTime(oldStart, 1000);
                // For rule B, assume a tumbling window of size 3000
                case "B": return HoppingWindowStartTime(oldStart, 3000);
                // For rule F, assume a hopping window of size 10000, hop 1000
                case "F": return HoppingWindowStartTime(oldStart, 1000);
            }
            throw new InvalidOperationException();
        }

        private static long AssignDuration(string ruleKey, long oldStart)
        {
            switch (ruleKey)
            {
                // For rule A, assume a tumbling window of size 1000
                case "A": return 1000;
                // For rule B, assume a tumbling window of size 3000
                case "B": return 3000;
                // For rule F, assume a hopping window of size 10000, hop 1000
                case "F": return 10000;
            }
            throw new InvalidOperationException();
        }

        private static IEnumerable<ValueTuple<string, int>> ComputeResult(string ruleKey, SortedMultiSet<int> state)
        {
            switch (ruleKey)
            {
                case "A":
                    // For rule A, assume we want a maximum and a minimum
                    return new ValueTuple<string, int>[]
                    {
                        ValueTuple.Create("Max", state.Last()),
                        ValueTuple.Create("Min", state.First())
                    };

                case "B":
                    // For rule B, assume a count and a count distinct
                    return new ValueTuple<string, int>[]
                    {
                        ValueTuple.Create("Count", (int)state.TotalCount),
                        ValueTuple.Create("Distinct", (int)state.UniqueCount)
                    };

                case "F":
                    // For rule F, assume a sum
                    return new ValueTuple<string, int>[]
                    {
                        ValueTuple.Create("Sum", state.GetEnumerable().Sum())
                    };
            }
            throw new InvalidOperationException();
        }

        public static void Main(string[] args)
        {
            var rulesEngine = RulesEngine(
                GetObservable().ToStreamable(),
                AssignRules,
                AssignStartEdge,
                AssignDuration,
                t => t.Item2,
                ComputeResult,
                0);
            var observer = new TestObserver<PartitionedStreamEvent<string, ValueTuple<string, int>>>(
                rulesEngine.ToStreamEventObservable());
        }
    }
}
