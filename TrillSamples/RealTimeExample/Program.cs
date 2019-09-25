// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace RealTimeExample
{
    public sealed class Program
    {
        public static void Main(string[] args)
        {
            // Poll performance counter 4 times a second
            var pollingInterval = TimeSpan.FromSeconds(0.25);

            // Take the total processor utilization performance counter
            string categoryName = "Processor";
            string counterName = "% Processor Time";
            string instanceName = "_Total";

            // Create an observable that feeds the performance counter periodically
            IObservable<PerformanceCounterSample> source =
                new PerformanceCounterObservable(categoryName, counterName, instanceName, pollingInterval);

            // Load the observable as a stream in Trill, injecting a punctuation every second. Because we use
            // FlushPolicy.FlushOnPunctuation, this will also flush the data every second.
            var inputStream =
                source.Select(e => StreamEvent.CreateStart(e.StartTime.Ticks, e))
                .ToStreamable(
                    null,
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time((ulong)TimeSpan.FromSeconds(1).Ticks));

            // Query 1: Aggregate query
            long windowSize = TimeSpan.FromSeconds(2).Ticks;
            var query1 = inputStream.TumblingWindowLifetime(windowSize).Average(e => e.Value);

            // Query 2: look for pattern of [CPU < 10] --> [CPU >= 10]
            var query2 = inputStream
                .AlterEventDuration(TimeSpan.FromSeconds(10).Ticks)
                .Detect(default(Tuple<float, float>), // register to store CPU value
                    p => p
                    .SingleElement(e => e.Value < 10, (ts, ev, reg) => new Tuple<float, float>(ev.Value, 0))
                    .SingleElement(e => e.Value >= 10, (ts, ev, reg) => new Tuple<float, float>(reg.Item1, ev.Value)));

            // Query 3: look for pattern of [k increasing CPU values --> drop CPU], report max, k
            int k = 2;
            var query3 = inputStream
                .AlterEventDuration(TimeSpan.FromSeconds(10).Ticks)
                .Detect(default(Tuple<float, int>), // register to store CPU value, incr count
                    p => p
                    .KleenePlus(e =>
                    e.SingleElement((ev, reg) => reg == null || ev.Value > reg.Item1, (ts, ev, reg) => new Tuple<float, int>(ev.Value, reg == null ? 1 : reg.Item2 + 1)))
                    .SingleElement((ev, reg) => ev.Value < reg.Item1 && reg.Item2 > k, (ts, ev, reg) => reg),
                    allowOverlappingInstances: false);

            // Egress results and write to console
            query3.ToStreamEventObservable().ForEachAsync(e => WriteEvent(e)).Wait();
        }

        private static void WriteEvent<T>(StreamEvent<T> e)
        {
            if (e.IsData)
            {
                Console.WriteLine($"EventKind = {e.Kind, 8}\t" +
                    $"StartTime = {new DateTime(e.StartTime)}\t" +
                    // "EndTime = {new DateTime(e.EndTime)}\t" +
                    $"Payload = ( {e.Payload.ToString()} )");
            }
            else // IsPunctuation
            {
                Console.WriteLine($"EventKind = {e.Kind}\tSyncTime  = {new DateTime(e.StartTime)}");
            }
        }
    }
}
