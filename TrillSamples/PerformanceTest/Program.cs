// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Runtime.InteropServices;
using Microsoft.StreamProcessing;

namespace PerformanceTest
{
    internal static class Program
    {
#if DEBUG
        private const int TotalInputEvents = 1000000;
#else
        private const int TotalInputEvents = 50000000;
#endif
        private const int NumRepeats = 1;
        private const int NumEventsPerTumble = 2000;

        internal struct Payload
        {
            public long field1;
            public long field2;
        }

        private static void ProcessQuery<P>(IStreamable<Empty, P> query, string name)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            Console.WriteLine("Query: {0}", name);

            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < NumRepeats; i++)
            {
                using (var result = query.Cache())
                {
                }
            }
            GC.Collect();
            GC.WaitForPendingFinalizers();
            sw.Stop();

            Console.WriteLine("Throughput: {0} K ev/sec", ((long)NumRepeats * TotalInputEvents) / sw.ElapsedMilliseconds);
        }

        public static void Main(string[] args)
        {
            // Affinitize thread to core 0
            NativeMethods.AffinitizeThread(0);

            // Load a sample dataset into main memory
            Console.WriteLine("Caching the input dataset into main memory...");
            var sw0 = new Stopwatch();
            sw0.Start();

            var tumblingWindowDataset =
                Observable.Range(0, TotalInputEvents)
                    .Select(e => StreamEvent.CreateInterval(((long)e / NumEventsPerTumble) * NumEventsPerTumble, ((long)e / NumEventsPerTumble) * NumEventsPerTumble + NumEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, NumEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1)
                    .Cache();

            var startEdgeOnlyDataset = tumblingWindowDataset
                                            .AlterEventDuration(StreamEvent.InfinitySyncTime)
                                            .SetProperty().IsSnapshotSorted(true, e => e.field1)
                                            .Cache();

            var singleSnapshotDataset = tumblingWindowDataset
                                            .AlterEventLifetime(a => StreamEvent.MinSyncTime, StreamEvent.InfinitySyncTime)
                                            .SetProperty().IsSnapshotSorted(true, e => e.field1)
                                            .Cache();

            sw0.Stop();
            Console.WriteLine("Time to load cache with {0} tuples: {1} sec", TotalInputEvents, sw0.Elapsed);
            Console.ReadLine();


            Console.WriteLine("\n**** Queries over dataset with tumbling windows ****");
            ProcessQuery(tumblingWindowDataset.Where(e => e.field1 != 0), "input.Where(e => e.field1 != 0)");
            ProcessQuery(tumblingWindowDataset.Select(e => e.field1), "input.Select(e => e.field1)");
            ProcessQuery(tumblingWindowDataset.Count(), "input.Count()");
            ProcessQuery(tumblingWindowDataset.Sum(e => e.field2), "input.Sum(e => e.field2)");
            ProcessQuery(tumblingWindowDataset.Average(e => e.field1), "input.Average(e => e.field1)");
            ProcessQuery(tumblingWindowDataset.Max(e => e.field1), "input.Max(e => e.field1)");
            ProcessQuery(tumblingWindowDataset.Min(e => e.field1), "input.Min(e => e.field1)");
            ProcessQuery(tumblingWindowDataset
                .GroupApply(
                    e => e.field2,
                    str => str.Count(),
                    (g, c) => new { Key = g, Count = c }),
                "input.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c })");

            Console.WriteLine("\n**** Queries over start-edge-only dataset ****");
            ProcessQuery(startEdgeOnlyDataset.Count(), "input.Count()");
            ProcessQuery(startEdgeOnlyDataset.Sum(e => e.field2), "input.Sum(e => e.field2)");
            ProcessQuery(startEdgeOnlyDataset.Average(e => e.field1), "input.Average(e => e.field1)");
            ProcessQuery(startEdgeOnlyDataset.Max(e => e.field1), "input.Max(e => e.field1)");
            ProcessQuery(startEdgeOnlyDataset.Min(e => e.field1), "input.Min(e => e.field1)");

            Console.WriteLine("\n**** Queries over a single-snapshot (atemporal) dataset ****");
            ProcessQuery(singleSnapshotDataset.Count(), "input.Count()");
            ProcessQuery(singleSnapshotDataset.Sum(e => e.field2), "input.Sum(e => e.field2)");
            ProcessQuery(singleSnapshotDataset.Average(e => e.field1), "input.Average(e => e.field1)");
            ProcessQuery(singleSnapshotDataset.Max(e => e.field1), "input.Max(e => e.field1)");
            ProcessQuery(singleSnapshotDataset.Min(e => e.field1), "input.Min(e => e.field1)");

            /* Dispose off the caches */
            tumblingWindowDataset.Dispose();
            startEdgeOnlyDataset.Dispose();
            singleSnapshotDataset.Dispose();

            Console.WriteLine("Press <ENTER>");
            Console.ReadLine();
        }
    }

    public static class NativeMethods
    {
        [DllImport("kernel32")]
        internal static extern uint GetCurrentThreadId();
        internal static void AffinitizeThread(int processor)
        {
            uint utid = GetCurrentThreadId();
            foreach (ProcessThread pt in System.Diagnostics.Process.GetCurrentProcess().Threads)
            {
                if (utid == pt.Id)
                {
                    long AffinityMask = 1 << processor;
                    pt.ProcessorAffinity = (IntPtr)(AffinityMask); // Set affinity for this
                }
            }
        }


    }
}
