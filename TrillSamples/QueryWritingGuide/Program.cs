// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace QueryWritingGuide
{
    public class Program
    {
        private struct ContextSwitch
        {
            public ContextSwitch(long tick, long processId, long cpuId, long cpuTemp)
            {
                this.Tick = tick;
                this.ProcessId = processId;
                this.CpuId = cpuId;
                this.CpuTemp = cpuTemp;
            }

            public long Tick;
            public long ProcessId;
            public long CpuId;
            public long CpuTemp;

            public override string ToString() =>
                $"Tick = {this.Tick, 4}\t" +
                $"ProcessId = {this.ProcessId}\t" +
                $"CpuId = {this.CpuId}\t" +
                $"CpuTemp = {this.CpuTemp}";
        };

        private struct ProcessName
        {
            public ProcessName(long processId, string processName)
            {
                this.ProcessId = processId;
                this.Name = processName;
            }

            public long ProcessId;
            public string Name;

            public override string ToString() =>
                $"ProcessId: {this.ProcessId}, " +
                $"ProcessName: {this.Name}";
        };

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

        public static void Main(string[] args)
        {
            // This program is a supplement to the Trill Query Writing Guide

            #region Section 2

            IObservable<ContextSwitch> contextSwitchObservable = new[]
            {
                new ContextSwitch(0, 1, 1, 120),
                new ContextSwitch(0, 3, 2, 121),
                new ContextSwitch(0, 5, 3, 124),
                new ContextSwitch(120, 2, 1, 123),
                new ContextSwitch(300, 1, 1, 122),
                new ContextSwitch(1800, 4, 2, 125),
                new ContextSwitch(3540, 2, 1, 119),
                new ContextSwitch(3600, 1, 1, 120),
            }.ToObservable();

            Console.WriteLine("Figure 11: Complete Pass-Through Trill Query Program");

            IObservable<StreamEvent<ContextSwitch>> contextSwitchStreamEventObservable =
                contextSwitchObservable.Select(e => StreamEvent.CreateInterval(e.Tick, e.Tick + 1, e));

            IObservableIngressStreamable<ContextSwitch> contextSwitchIngressStreamable =
                contextSwitchStreamEventObservable.ToStreamable(DisorderPolicy.Drop());
            var contextSwitchStreamable =
                (IStreamable<Empty, ContextSwitch>)contextSwitchIngressStreamable;

            IObservable<StreamEvent<ContextSwitch>> passthroughContextSwitchStreamEventObservable =
                contextSwitchStreamable.ToStreamEventObservable();

            passthroughContextSwitchStreamEventObservable.Where(e => e.IsData)
                .ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 12: Non-StreamEvent Passthrough");

            var payloadStreamable = contextSwitchObservable.ToTemporalStreamable(cs => cs.Tick);
            var passthroughPayloadObservable = payloadStreamable.ToTemporalObservable((start, cs) => cs);
            passthroughPayloadObservable.ForEachAsync(cs => Console.WriteLine(cs.ToString())).Wait();
            Console.WriteLine();

            #endregion

            #region Section 3

            Console.WriteLine("Figure 14: Where Query Code");

            var contextSwitchTwoCores = contextSwitchStreamable.Where(p => p.CpuId == 1 || p.CpuId == 2);
            contextSwitchTwoCores.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => WriteEvent(e))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 17: Select Query Code");

            var contextSwitchTwoCoresNoTemp = contextSwitchTwoCores.Select(
                    e => new { e.Tick, e.ProcessId, e.CpuId });
            contextSwitchTwoCoresNoTemp.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\t" +
                    $"Tick = {e.Payload.Tick, 4}\tProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}"))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 18: Alternate Select Query Code");

            contextSwitchTwoCoresNoTemp = contextSwitchTwoCores.Select(
                    e => new { e.Tick, e.ProcessId, e.CpuId });
            contextSwitchTwoCoresNoTemp.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\t" +
                    $"Tick = {e.Payload.Tick, 4}\tProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}"))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 20: Where and Select with the LINQ Comprehension Syntax");

            contextSwitchTwoCoresNoTemp = from e in contextSwitchStreamable
                                          where e.CpuId == 1 || e.CpuId == 2
                                          select new { e.Tick, e.ProcessId, e.CpuId };
            contextSwitchTwoCoresNoTemp.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\t" +
                    $"Tick = {e.Payload.Tick, 4}\tProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 4

            Console.WriteLine("Figure 23: Join Query Code");

            var processNamesObservable = new[]
            {
                new ProcessName(1, "Word"),
                new ProcessName(2, "Internet Explorer"),
                new ProcessName(3, "Excel"),
                new ProcessName(4, "Visual Studio"),
                new ProcessName(5, "Outlook"),
            }.ToObservable();
            var namesStream = processNamesObservable.
                    Select(e => StreamEvent.CreateInterval(0, 10000, e)).
                    ToStreamable();
            var contextSwitchWithNames = contextSwitchTwoCoresNoTemp.Join(namesStream,
                e => e.ProcessId, e => e.ProcessId,
                (leftPayload, rightPayload) => new
                {
                    leftPayload.Tick,
                    leftPayload.ProcessId,
                    leftPayload.CpuId,
                    rightPayload.Name
                });
            contextSwitchWithNames.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\t" +
                    $"Tick = {e.Payload.Tick, 4}\tProcessId = {e.Payload.ProcessId}\t" +
                    $"CpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}"))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 25: Join Query Comprehension Syntax");

            contextSwitchWithNames = from leftPayload in contextSwitchTwoCoresNoTemp
                                     join rightPayload in namesStream on
                                         leftPayload.ProcessId equals rightPayload.ProcessId
                                     select new
                                     {
                                         leftPayload.Tick,
                                         leftPayload.ProcessId,
                                         leftPayload.CpuId,
                                         rightPayload.Name
                                     };
            contextSwitchWithNames.ToStreamEventObservable().Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\t" +
                    $"Tick = {e.Payload.Tick, 4}\tProcessId = {e.Payload.ProcessId}\t" +
                    $"CpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}"))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 26: Entire Join Query Comprehension Syntax");

            contextSwitchWithNames = from leftPayload in contextSwitchStreamable
                                     join rightPayload in namesStream on
                                         leftPayload.ProcessId equals rightPayload.ProcessId
                                     where leftPayload.CpuId == 1 || leftPayload.CpuId == 2
                                     select new
                                     {
                                         leftPayload.Tick,
                                         leftPayload.ProcessId,
                                         leftPayload.CpuId,
                                         rightPayload.Name
                                     };
            contextSwitchWithNames.ToStreamEventObservable().Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\t" + $"Tick = {e.Payload.Tick, 4}\t" +
                    $"ProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}"))
                    .Wait();
            Console.WriteLine();

            #endregion

            #region Section 5

            Console.WriteLine("Figure 27: AlterEventDuration Query Code");

            var infiniteContextSwitch = contextSwitchWithNames.AlterEventDuration(StreamEvent.InfinitySyncTime);
            infiniteContextSwitch.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 20}\tTick = {e.Payload.Tick, 4}\t" +
                    $"ProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 6

            Console.WriteLine("Figure 29: ClipEventDuration Query Code");

            var clippedContextSwitch = infiniteContextSwitch
                .ClipEventDuration(infiniteContextSwitch, e => e.CpuId, e => e.CpuId);
            clippedContextSwitch.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 20}\tTick = {e.Payload.Tick, 4}\t" +
                    $"ProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 7

            Console.WriteLine("Figure 31: Multicast Version of ClipEventDuration Query Code");

            clippedContextSwitch = infiniteContextSwitch.Multicast(
                    s => s.ClipEventDuration(s, e => e.CpuId, e => e.CpuId));
            clippedContextSwitch.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 20}\tTick = {e.Payload.Tick, 4}\t" +
                    $"ProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 8

            Console.WriteLine("Figure 33: ShiftEventLifetime Query Code");

            var shiftedClippedContextSwitch = clippedContextSwitch.ShiftEventLifetime(1);
            shiftedClippedContextSwitch.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 20}\tTick = {e.Payload.Tick, 4}\t" +
                    $"ProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}"))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 35: Timeslices Query Code");

            var timeslices = shiftedClippedContextSwitch.Join(contextSwitchWithNames,
                e => e.CpuId, e => e.CpuId,
                (left, right) => new
                {
                    left.ProcessId,
                    left.CpuId,
                    left.Name,
                    Timeslice = right.Tick - left.Tick
                });
            timeslices.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\tProcessId = {e.Payload.ProcessId}\t" +
                    $"CpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}\tTimeslice = {e.Payload.Timeslice}"))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 37: Timeslices Query Code Using Multicast");

            timeslices = contextSwitchWithNames.Multicast(t => t
                .AlterEventDuration(StreamEvent.InfinitySyncTime)
                .Multicast(s => s
                    .ClipEventDuration(s, e => e.CpuId, e => e.CpuId))
                    .ShiftEventLifetime(1)
                    .Join(t,
                        e => e.CpuId, e => e.CpuId,
                        (left, right) => new
                        {
                            left.ProcessId,
                            left.CpuId,
                            left.Name,
                            Timeslice = right.Tick - left.Tick
                        }));
            timeslices.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\tProcessId = {e.Payload.ProcessId}\t" +
                    $"CpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}\tTimeslice = {e.Payload.Timeslice}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 9

            Console.WriteLine("Figure 41: Rassigning Timeslice Lifetimes with AlterEventLifetime");

            var timeslicesForProcess1Cpu1 = timeslices.Where(e => e.ProcessId == 1 && e.CpuId == 1);
            var windowedTimeslicesForProcess1Cpu1 = timeslicesForProcess1Cpu1.
                    AlterEventLifetime(origStartTime => (1 + ((origStartTime - 1) / 3600)) * 3600, 3600);
            windowedTimeslicesForProcess1Cpu1.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\tProcessId = {e.Payload.ProcessId}\t" +
                    $"CpuId = {e.Payload.CpuId}\tName = {e.Payload.Name}\tTimeslice = {e.Payload.Timeslice}"))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 42: Reassigning Lifetimes with HoppingWindowLifetime");

            var windowedTimeslices2 = timeslicesForProcess1Cpu1.HoppingWindowLifetime(3600, 3600);
            windowedTimeslices2.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\tProcessId = {e.Payload.ProcessId}\t" +
                    $"CpuId = {e.Payload.CpuId}\tName = {e.Payload.Name}\tTimeslice = {e.Payload.Timeslice}"))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 43: Sum Query Code");

            var totalConsumptionPerPeriodForProcess1Cpu1 = windowedTimeslicesForProcess1Cpu1.Sum(e => e.Timeslice);
            totalConsumptionPerPeriodForProcess1Cpu1.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\tPayload = {e.Payload}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 10

            Console.WriteLine("Figure 45: Group and Apply Query Code");

            var totalConsumptionPerPeriod = timeslices.GroupApply(
                    e => new { e.CpuId, e.ProcessId, e.Name },
                    s => s.HoppingWindowLifetime(3600, 3600).Sum(e => e.Timeslice),
                    (g, p) => new { g.Key.CpuId, g.Key.Name, TotalTime = p });
            totalConsumptionPerPeriod.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\t" +
                    $"CpuId = {e.Payload.CpuId}\tName = {e.Payload.Name, 18}\tTotalTime = {e.Payload.TotalTime}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 11

            Console.WriteLine("Figure 50: Chop Query Code");
            Console.WriteLine("NB: This query is expected to fail!");

            var alternativeContextSwitchStream = new[]
            {
                new ContextSwitch(0, 1, 1, 120),
                new ContextSwitch(0, 3, 2, 121),
                new ContextSwitch(0, 5, 3, 124),
                new ContextSwitch(120, 2, 1, 123),
                new ContextSwitch(300, 1, 1, 122),
                new ContextSwitch(1800, 4, 2, 125),
                new ContextSwitch(3540, 2, 1, 119),
                new ContextSwitch(3600, 1, 1, 120),
                new ContextSwitch(5400, 3, 2, 122),
                new ContextSwitch(7200, 4, 2, 121),
            }.ToObservable()
                .Select(e => StreamEvent.CreateInterval(e.Tick, e.Tick + 1, e))
                .ToStreamable(DisorderPolicy.Drop());

            var contextSwitchChoppedUnbounded = alternativeContextSwitchStream
                .AlterEventDuration(StreamEvent.InfinitySyncTime)
                .Multicast(s => s.ClipEventDuration(s, e => e.CpuId, e => e.CpuId))
                .Chop(0, 3600)
                .Select((origStartTime, e) =>
                    new { Tick = origStartTime, e.ProcessId, e.CpuId, e.CpuTemp })
                .AlterEventDuration(1);
            try
            {
                contextSwitchChoppedUnbounded.ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ForEachAsync(e => Console.WriteLine(
                        $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\tTick{e.Payload.Tick, 4}\t" +
                        $"ProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}\tCPUTemp{e.Payload.CpuTemp}"))
                    .Wait();
            }
            catch (AggregateException e)
            {
                foreach (var ex in e.InnerExceptions) Console.WriteLine("{0}", ex.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0}", e.Message);
            }
            Console.WriteLine();

            Console.WriteLine("Figure 52: Improved Chop Query Code");

            var fixedInterval = new[] { StreamEvent.CreateInterval(0, 10800, Unit.Default) }
                            .ToObservable().ToStreamable();
            var contextSwitchChopped = alternativeContextSwitchStream
                    .AlterEventDuration(StreamEvent.InfinitySyncTime)
                    .Multicast(s => s.ClipEventDuration(s, e => e.CpuId, e => e.CpuId))
                    .Join(fixedInterval, (left, right) => left)
                    .Chop(0, 3600)
                    .Select((origStartTime, e) =>
                        new { e.CpuId, e.ProcessId, e.CpuTemp, Tick = origStartTime })
                    .AlterEventDuration(1);
            contextSwitchChopped.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"StartTime = {e.StartTime, 4}\tEndTime = {e.EndTime, 4}\tTick = {e.Payload.Tick, 4}\t" +
                    $"ProcessId = {e.Payload.ProcessId}\tCpuId = {e.Payload.CpuId}\tCPUTemp = {e.Payload.CpuTemp}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 12

            Console.WriteLine("Figure 54: Final Query");

            var choppedContextSwitch = alternativeContextSwitchStream
                    .Where(cs => cs.CpuId == 1 || cs.CpuId == 2)
                    .AlterEventDuration(StreamEvent.InfinitySyncTime)
                    .Multicast(s => s.ClipEventDuration(s, e => e.CpuId, e => e.CpuId))
                    .Join(fixedInterval, (left, right) => left)
                    .Chop(0, 3600)
                    .Select((start, e) => new { Tick = start, e.ProcessId, e.CpuId, e.CpuTemp })
                    .AlterEventDuration(1);

            var choppedContextSwitchWithNames = choppedContextSwitch
                .Join(namesStream, e => e.ProcessId, e => e.ProcessId, (left, right) => new
                    {
                        left.Tick,
                        left.ProcessId,
                        left.CpuId,
                        right.Name
                    });

            var timeslicesPerCpu = choppedContextSwitchWithNames
                .Multicast(t => t
                    .AlterEventDuration(StreamEvent.InfinitySyncTime)
                    .Multicast(s => s.ClipEventDuration(s, e => e.CpuId, e => e.CpuId))
                    .ShiftEventLifetime(1)
                    .Join(t,
                        e => e.CpuId, e => e.CpuId,
                        (left, right) => new
                        {
                            left.ProcessId,
                            left.CpuId,
                            left.Name,
                            Timeslice = right.Tick - left.Tick
                        }));

            var mostCpuConsumedPerPeriod = timeslicesPerCpu
                .GroupApply(
                    e => new { e.ProcessId, e.Name },
                    s => s.HoppingWindowLifetime(3600, 3600).Sum(e => e.Timeslice),
                    (g, p) => new { g.Key.Name, TotalTime = p })
                .Max((l, r) => l.TotalTime.CompareTo(r.TotalTime))
                .Select((startTime, payload) => new
                    {
                        PeriodStart = startTime - 3600,
                        PeriodEnd = startTime,
                        ProcessName = payload.Name,
                        TotalCpuConsumed = payload.TotalTime
                    });

            mostCpuConsumedPerPeriod.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine(
                    $"PeriodStart = {e.Payload.PeriodStart, 4}\tPeriodEnd = {e.Payload.PeriodEnd, 4}\t" +
                    $"Name = {e.Payload.ProcessName}\tTotalTime = {e.Payload.TotalCpuConsumed}"))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 13.1

            Console.WriteLine("Figure 56: Out of Order Input (Throw)");

            var outOfOrderStreamableThrow = new[]
            {
                StreamEvent.CreateInterval(10, 100, 1),
                StreamEvent.CreateInterval(0, 50, 2),
                StreamEvent.CreateInterval(0, 10, 3),
                StreamEvent.CreateInterval(11, 90, 4)
            }.ToObservable().ToStreamable(DisorderPolicy.Throw());
            try
            {
                outOfOrderStreamableThrow.ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ForEachAsync(e => WriteEvent(e))
                    .Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine("{0}", e.Message);
            }
            Console.WriteLine();

            Console.WriteLine("Figure 57: Out of Order Input (Drop)");

            var outOfOrderStreamableDrop = new[]
            {
                StreamEvent.CreateInterval(10, 100, 1),
                StreamEvent.CreateInterval(0, 50, 2),
                StreamEvent.CreateInterval(0, 10, 3),
                StreamEvent.CreateInterval(11, 90, 4)
            }.ToObservable().ToStreamable(DisorderPolicy.Drop());
            outOfOrderStreamableDrop.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => WriteEvent(e))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 58: Out of Order Input (Adjust)");

            var outOfOrderStreamableAdjust = new[]
            {
                StreamEvent.CreateInterval(10, 100, 1),
                StreamEvent.CreateInterval(0, 50, 2),
                StreamEvent.CreateInterval(0, 10, 3),
                StreamEvent.CreateInterval(11, 90, 4)
            }.ToObservable().ToStreamable(DisorderPolicy.Adjust());
            outOfOrderStreamableAdjust.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => WriteEvent(e))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 13.3

            Console.WriteLine("Figure 60: Creating namesStream with Edge Events");

            namesStream = processNamesObservable.Select(e => StreamEvent.CreateStart(0, e)).
                    Concat(processNamesObservable.Select(e => StreamEvent.CreateEnd(10000, 0, e))).
                    ToStreamable();
            namesStream.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => WriteEvent(e))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 61: Creating namesStream using Interval and Edge Events");

            var namesObservable1 = new[]
            {
                new ProcessName(1, "Word"),
                new ProcessName(2, "Internet Explorer"),
                new ProcessName(3, "Excel"),
            }.ToObservable();
            var namesObservable2 = new[]
            {
                new ProcessName(4, "Visual Studio"),
                new ProcessName(5, "Outlook"),
            }.ToObservable();
            namesStream = namesObservable1.Select(e => StreamEvent.CreateInterval(0, 10000, e))
                    .Concat(namesObservable2.Select(e => StreamEvent.CreateStart(0, e)))
                    .Concat(namesObservable2.Select(e => StreamEvent.CreateEnd(10000, 0, e)))
                    .ToStreamable();
            namesStream.ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => WriteEvent(e))
                .Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 62: Coalescing Matching Edges");

            namesStream.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ForEachAsync(e => WriteEvent(e))
                .Wait();
            Console.WriteLine();

            #endregion

            #region Section 13.4

            Console.WriteLine("Figure 63: Input with Punctuations");

            var streamablePunctuations = new[]
            {
                StreamEvent.CreateInterval(0, 1, 1),
                StreamEvent.CreateInterval(3, 4, 2),
                StreamEvent.CreatePunctuation<int>(10),
                StreamEvent.CreatePunctuation<int>(20),
                StreamEvent.CreatePunctuation<int>(30),
                StreamEvent.CreatePunctuation<int>(40),
                StreamEvent.CreateInterval(40, 41, 3)
            }.ToObservable().ToStreamable(DisorderPolicy.Drop(), FlushPolicy.FlushOnPunctuation,
                                          null, OnCompletedPolicy.None);
            streamablePunctuations.ToStreamEventObservable()
                .ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 64: Input with Generated Punctuations After 10 Tick Periods");

            var streamableTimePeriodEventPunctuations = new[]
            {
                StreamEvent.CreateInterval(0, 1, 1),
                StreamEvent.CreateInterval(10, 4, 2),
                StreamEvent.CreateInterval(19, 4, 3),
                StreamEvent.CreateInterval(40, 41, 4)
            }.ToObservable().ToStreamable(DisorderPolicy.Drop(), FlushPolicy.FlushOnPunctuation,
                                          PeriodicPunctuationPolicy.Time(10), OnCompletedPolicy.None);
            streamableTimePeriodEventPunctuations.ToStreamEventObservable()
                .ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 65: Query with No Output");

            var incompleteOutputQuery = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2)
            }.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Count()
                .ToStreamEventObservable();
            incompleteOutputQuery.ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 66: Query with Output up to Time 1");

            incompleteOutputQuery = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2),
                StreamEvent.CreatePunctuation<int>(1)
            }.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Count()
                .ToStreamEventObservable();
            incompleteOutputQuery.ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 67: Alternate Query with Output up to Time 1");

            incompleteOutputQuery = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2)
            }.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.Flush)
                .Count()
                .ToStreamEventObservable();
            incompleteOutputQuery.ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 68: Query with all Output");

            var completeOutputQuery = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Count()
                .ToStreamEventObservable();
            completeOutputQuery.ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 69: Alternate Query with all Output");

            completeOutputQuery = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2)
            }.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.EndOfStream)
                .Count()
                .ToStreamEventObservable();
            completeOutputQuery.ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            #endregion

            Console.ReadLine();
        }
    }
}