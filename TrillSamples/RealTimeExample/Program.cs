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

            // The query
            long windowSize = TimeSpan.FromSeconds(2).Ticks;
            var query = inputStream.AlterEventDuration(windowSize).Average(e => e.Value);

            // Egress results and write to console
            query.ToStreamEventObservable().ForEachAsync(e => WriteEvent(e)).Wait();
        }

        private static void WriteEvent<T>(StreamEvent<T> e)
        {
            if (e.IsData)
            {
                Console.WriteLine($"EventKind = {e.Kind, 8}\t" +
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 20}\t" +
                    $"Payload = ( {e.Payload.ToString()} )");
            }
            else // IsPunctuation
            {
                Console.WriteLine($"EventKind = {e.Kind}\tSyncTime  = {e.StartTime, 4}");
            }
        }
    }
}
