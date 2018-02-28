using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using System.Reactive;

namespace QueryWritersGuide
{
    class Program
    {
        struct ContextSwitch
        {
            public ContextSwitch(long inCSTicks, long inPID, long inCID, long inCPUTemp)
            {
                CSTicks = inCSTicks;
                PID = inPID;
                CID = inCID;
                CPUTemp = inCPUTemp;
            }

            public long CSTicks;
            public long PID;
            public long CID;
            public long CPUTemp;
        };

        struct ProcessName
        {
            public ProcessName(long inPID, string inPName)
            {
                PID = inPID;
                PName = inPName;
            }

            public long PID;
            public string PName;
        };

        static void WriteEvent(StreamEvent<ProcessName> e)
        {
            if (e.IsInterval)
            {
                Console.WriteLine(
                    "Event Kind=Interval\tStart Time={0}\tEnd Time={1}\tPID={2}\tPName={3}",
                    e.StartTime, e.EndTime, e.Payload.PID, e.Payload.PName);
            }
            else if (e.IsStart)
            {
                Console.WriteLine(
                    "Event Kind=Start\tStart Time={0}\tEnd Time=????\tPID={1}\tPName={2}",
                    e.StartTime, e.Payload.PID, e.Payload.PName);
            }
            else if (e.IsEnd)
            {
                Console.WriteLine(
                    "Event Kind=End\t\tStart Time={0}\tEnd Time={1}\tPID={2}\tPName={3}",
                    e.StartTime, e.EndTime, e.Payload.PID, e.Payload.PName);
            }
            else // Is a punctuation
            {
                Console.WriteLine(
                  "Event Kind=Punctuation\tSync Time={0}",
                  e.SyncTime);
            }
        }

        static void WriteEvent(StreamEvent<int> e)
        {
            if (e.IsInterval)
            {
                Console.WriteLine(
                    "Event Kind=Interval\tStart Time={0}\tEnd Time={1}\tVal={2}",
                    e.StartTime, e.EndTime, e.Payload);
            }
            else if (e.IsStart)
            {
                Console.WriteLine(
                    "Event Kind=Start\tStart Time={0}\tEnd Time=????\tVal={1}",
                    e.StartTime, e.Payload);
            }
            else if (e.IsEnd)
            {
                Console.WriteLine(
                    "Event Kind=End\t\tStart Time={0}\tEnd Time={1}\tVal={2}",
                    e.StartTime, e.EndTime, e.Payload);
            }
            else // Is a punctuation
            {
                Console.WriteLine(
                  "Event Kind=Punctuation\tSync Time={0}",
                  e.SyncTime);
            }
        }

        static void WriteEvent(StreamEvent<ulong> e)
        {
            if (e.IsData)
            {
                if (e.IsInterval)
                {
                    Console.WriteLine(
                        "Event Kind=Interval\tStart Time={0}\tEnd Time={1}\tVal={2}",
                        e.StartTime, e.EndTime, e.Payload);
                }
                else if (e.IsStart)
                {
                    Console.WriteLine(
                        "Event Kind=Start\tStart Time={0}\tEnd Time=????\tVal={1}",
                        e.StartTime, e.Payload);
                }
                else if (e.IsEnd)
                {
                    Console.WriteLine(
                        "Event Kind=End\t\tStart Time={0}\tEnd Time={1}\tVal={2}",
                        e.StartTime, e.EndTime, e.Payload);
                }
            }
            else
            {
                Console.WriteLine(
                  "Event Kind=Punctuation Sync Time={0}",
                  e.SyncTime);
            }
        }

        static void Main(string[] args)
        {
            // This program is a supplement to the Trill Users Guide

            #region Section 2

            IObservable<ContextSwitch> cSTicksObs = new[]
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

            var cSTicksEventObs = cSTicksObs.Select(
            e => StreamEvent.CreateInterval(e.CSTicks, e.CSTicks + 1, e));
            var cSTicksStream =
                    cSTicksEventObs.ToStreamable(OnCompletedPolicy.EndOfStream(), 
                                                 DisorderPolicy.Drop());
            var origCSTicksEventObs = cSTicksStream.ToStreamEventObservable();
            origCSTicksEventObs.Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tCPUTemp={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID,
                    e.Payload.CPUTemp)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 3

            Console.WriteLine("Figure 13: Where Query Code");

            var cSTicks2Cores = cSTicksStream.Where(p => p.CID == 1 || p.CID == 2);
            cSTicks2Cores.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tCPUTemp={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID,
                    e.Payload.CPUTemp)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 16: Select Query Code");
            var cSNarrowTicks2Cores = cSTicks2Cores.Select(
                    e => new { CSTicks = e.CSTicks, PID = e.PID, CID = e.CID });
            cSNarrowTicks2Cores.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 17: Alternate Select Query Code");
            var cSNarrowTicks2Cores2 = cSTicks2Cores.Select(
                    e => new { CSTicks = e.CSTicks, PID = e.PID, CID = e.CID });
            cSNarrowTicks2Cores2.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 19: Where and Select with the LinQ Comprehension Syntax");
            var cSNarrowTicks2Cores3 = from e in cSTicksStream
                                       where e.CID == 1 || e.CID == 2
                                       select new { e.CSTicks, e.PID, e.CID };
            cSNarrowTicks2Cores3.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 4

            Console.WriteLine("Figure 22: Join Query Code");

            var processNamesObs = new[]
            {
                new ProcessName(1, "Word"),
                new ProcessName(2, "Internet Explorer"),
                new ProcessName(3, "Excel"),
                new ProcessName(4, "Visual Studio"),
                new ProcessName(5, "Outlook"),
            }.ToObservable();
            var pNamesStream = processNamesObs.
                    Select(e => StreamEvent.CreateInterval(0, 10000,e)).
                    ToStreamable(OnCompletedPolicy.EndOfStream());
            var cSTicks2CoresWithPNames = cSNarrowTicks2Cores.Join(pNamesStream, e => e.PID, e => e.PID,
                    (leftPayload, rightPayload) => new
                    {
                        leftPayload.CSTicks,
                        leftPayload.PID,
                        leftPayload.CID,
                        PName = rightPayload.PName
                    });
            cSTicks2CoresWithPNames.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tPName={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.PName)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 24: Join Query Comprehension Syntax");
            var cSTicks2CoresWithPNames2 = from leftPayload in cSNarrowTicks2Cores
                                           join rightPayload in pNamesStream on 
                                                   leftPayload.PID equals rightPayload.PID
                                           select new { leftPayload.CSTicks, 
                                                        leftPayload.PID, 
                                                        leftPayload.CID, 
                                                        PName = rightPayload.PName };
            cSTicks2CoresWithPNames2.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tPName={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.PName)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 25: Entire Join Query Comprehension Syntax");
            var cSTicks2CoresWithPNames3 = from leftPayload in cSTicksStream
                                           join rightPayload in pNamesStream on
                                                   leftPayload.PID equals rightPayload.PID
                                           where leftPayload.CID == 1 || leftPayload.CID == 2
                                           select new
                                           {
                                               leftPayload.CSTicks,
                                               leftPayload.PID,
                                               leftPayload.CID,
                                               rightPayload.PName
                                           };
            cSTicks2CoresWithPNames3.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tPName={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.PName)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 5

            Console.WriteLine("Figure 26: AlterEventDuration Query Code");
            var infinitecSTicks2Cores =
                    cSTicks2CoresWithPNames.AlterEventDuration(StreamEvent.InfinitySyncTime);
            infinitecSTicks2Cores.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tPName={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.PName)).Wait();
            Console.WriteLine();

            #endregion Section 6

            #region Section 6

            Console.WriteLine("Figure 28: ClipEventDuration Query Code");
            var clippedCSTicks2Cores = infinitecSTicks2Cores.
                    ClipEventDuration(infinitecSTicks2Cores, e => e.CID, e => e.CID);
            clippedCSTicks2Cores.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges()).Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tPName={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.PName)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 7

            Console.WriteLine("Figure 30: Multicast Version of ClipEventDuration Query Code");
            var clippedCSTicks2Cores2 = infinitecSTicks2Cores.Multicast(
                    s => s.ClipEventDuration(s, e => e.CID, e => e.CID));
            clippedCSTicks2Cores2.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges()).Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tPName={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.PName)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 8

            Console.WriteLine("Figure 32: ShiftEventLifetime Query Code");
            var shiftedClippedCS = clippedCSTicks2Cores.ShiftEventLifetime(startTime => 1);
            shiftedClippedCS.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges()).Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks={2}\tPID={3}\tCID={4}\tPName={5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.PName)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 34: Timeslices Query Code");
            var timeslices = shiftedClippedCS.Join(cSTicks2CoresWithPNames, e => e.CID, e => e.CID,
                    (left, right) => new
                    {
                        left.PID,
                        left.CID,
                        left.PName,
                        Timeslice = right.CSTicks - left.CSTicks
                    });
            timeslices.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges()).Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tPID={2}\tCID={3}\tPName={4}\tTimeslice={5}",
                    e.SyncTime, e.OtherTime, e.Payload.PID, e.Payload.CID, e.Payload.PName, e.Payload.Timeslice)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 36: Timeslices Query Code Using Multicast");
            var timeslices2 = cSTicks2CoresWithPNames.Multicast(
                    t => t.AlterEventDuration(StreamEvent.InfinitySyncTime).
                    Multicast(s => s.ClipEventDuration(s, e => e.CID, e => e.CID)).
                    ShiftEventLifetime(startTime => 1).
                    Join(t, e => e.CID, e => e.CID, (left, right) => new
                    {
                        left.PID,
                        left.CID,
                        left.PName,
                        Timeslice = right.CSTicks - left.CSTicks
                    }));
            timeslices2.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges()).Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tPID={2}\tCID={3}\tPName={4}\tTimeslice={5}",
                    e.SyncTime, e.OtherTime, e.Payload.PID, e.Payload.CID, e.Payload.PName, e.Payload.Timeslice)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 9

            Console.WriteLine("Figure 40: Rassigning Timeslice Lifetimes with AlterEventLifetime");
            var pID1CID1Timeslices = timeslices.Where(e => e.PID == 1 && e.CID == 1);
            var windowedTimeslices = pID1CID1Timeslices.
                    AlterEventLifetime(origStartTime => (1 + (origStartTime - 1) / 3600) * 3600, 3600);
            windowedTimeslices.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tPID={2}\tCID={3}\tPName={4}\tTimeslice={5}",
                    e.SyncTime, e.OtherTime, e.Payload.PID, e.Payload.CID, e.Payload.PName, e.Payload.Timeslice)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 41: Reassigning Lifetimes with HoppingWindowLifetime");
            var windowedTimeslices2 = pID1CID1Timeslices.HoppingWindowLifetime(3600, 3600);
            windowedTimeslices2.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tPID={2}\tCID={3}\tPName={4}\tTimeslice={5}",
                    e.SyncTime, e.OtherTime, e.Payload.PID, e.Payload.CID, e.Payload.PName, e.Payload.Timeslice)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 42: Sum Query Code");
            var totalConsumptionPerPeriod = windowedTimeslices.Sum(e => e.Timeslice);
            totalConsumptionPerPeriod.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges()).Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tPayload={2}",
                    e.SyncTime, e.OtherTime, e.Payload)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 10

            Console.WriteLine("Figure 44: Group and Apply Query Code");
            var totalConsumptionPerPeriodAllCoresAllProcesses = timeslices.GroupApply(
                    e => new { e.CID, e.PID, e.PName },
                    s => s.HoppingWindowLifetime(3600, 3600).Sum(e => e.Timeslice),
                   (g, p) => new { g.Key.CID, g.Key.PName, TotalTime = p });
            totalConsumptionPerPeriodAllCoresAllProcesses.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges()).Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCID={2}\tPName={3}\tTotaltime={4}",
                    e.SyncTime, e.OtherTime, e.Payload.CID, e.Payload.PName, e.Payload.TotalTime)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 11

            Console.WriteLine("Figure 49: Chop Query Code");
            Console.WriteLine("NB: This query is expected to fail!");
            var altCSTicksObs = new[]
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
            }.ToObservable();
            var altCSTicksEventObs = altCSTicksObs.Select(
            e => StreamEvent.CreateInterval(e.CSTicks, e.CSTicks + 1, e));
            var altCSTicksStream =
                    altCSTicksEventObs.ToStreamable(OnCompletedPolicy.EndOfStream(), DisorderPolicy.Drop());
            var cSTicksWithExtraCS = altCSTicksStream.AlterEventDuration(StreamEvent.InfinitySyncTime).
                    Multicast(s => s.ClipEventDuration(s, e => e.CID, e => e.CID)).
                    Chop(0, 3600).
                    Select((origStartTime, e) =>
                            new { CSTicks = origStartTime, e.PID, e.CID, e.CPUTemp }).
                    AlterEventDuration(1);
            try
            {
                cSTicksWithExtraCS.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                        "Start Time={0}\tEnd Time={1}\tCSTicks{2}\tPID={3}\tCID={4}\tCPUTemp{5}",
                        e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.CPUTemp)).Wait();
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

            Console.WriteLine("Figure 51: Improved Chop Query Code");
            var cSTicksWithExtraCSImp = altCSTicksStream.
                    AlterEventDuration(StreamEvent.InfinitySyncTime).
                    Multicast(s => s.ClipEventDuration(s, e => e.CID, e => e.CID)).
                    Join(new[] { StreamEvent.CreateInterval(0, 10800, Unit.Default) }.
                         ToObservable().ToStreamable(OnCompletedPolicy.EndOfStream()),
                         (left, right) => left).Chop(0, 3600).Select((origStartTime, e) =>
                           new { e.CID, e.PID, e.CPUTemp, CSTicks = origStartTime }).
                    AlterEventDuration(1);
            cSTicksWithExtraCSImp.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => Console.WriteLine(
                    "Start Time={0}\tEnd Time={1}\tCSTicks{2}\tPID={3}\tCID={4}\tCPUTemp{5}",
                    e.SyncTime, e.OtherTime, e.Payload.CSTicks, e.Payload.PID, e.Payload.CID, e.Payload.CPUTemp)).Wait();
            Console.WriteLine();

            #endregion

            #region Section 12

            Console.WriteLine("Figure 53: Out of Order Input (Throw)");
            var ooStreamableThrow = new[] 
            {
                StreamEvent.CreateInterval(10, 100, 1),
                StreamEvent.CreateInterval(0, 50, 2),
                StreamEvent.CreateInterval(0, 10, 3),
                StreamEvent.CreateInterval(11, 90, 4)
            }.ToObservable().ToStreamable(OnCompletedPolicy.EndOfStream(), DisorderPolicy.Throw());
            try
            {
                ooStreamableThrow.ToStreamEventObservable().Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine("{0}", e.Message);
            }
            Console.WriteLine();

            Console.WriteLine("Figure 54: Out of Order Input (Drop)");
            var ooStreamableDrop = new[]
            {
                StreamEvent.CreateInterval(10, 100, 1),
                StreamEvent.CreateInterval(0, 50, 2),
                StreamEvent.CreateInterval(0, 10, 3),
                StreamEvent.CreateInterval(11, 90, 4)
            }.ToObservable().ToStreamable(OnCompletedPolicy.EndOfStream(), DisorderPolicy.Drop());
            ooStreamableDrop.ToStreamEventObservable().Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 55: Out of Order Input (Adjust)");
            var ooStreamableAdjust = new[]
            {
                StreamEvent.CreateInterval(10, 100, 1),
                StreamEvent.CreateInterval(0, 50, 2),
                StreamEvent.CreateInterval(0, 10, 3),
                StreamEvent.CreateInterval(11, 90, 4)
            }.ToObservable().ToStreamable(OnCompletedPolicy.EndOfStream(), DisorderPolicy.Adjust());
            ooStreamableAdjust.ToStreamEventObservable().Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 57: Creating PNamesStream with Edge Events");
            var pNamesStream2 = processNamesObs.Select(e => StreamEvent.CreateStart(0, e)).
                    Concat(processNamesObs.Select(
                        e => StreamEvent.CreateEnd(10000, 0, e))).
                    ToStreamable(OnCompletedPolicy.EndOfStream());
            pNamesStream2.ToStreamEventObservable().Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 58: Creating PNameStream using Interval and Edge Events");
            var processNamesObs1 = new[]
            {
                new ProcessName(1, "Word"),
                new ProcessName(2, "Internet Explorer"),
                new ProcessName(3, "Excel"),
            }.ToObservable();
            var processNamesObs2 = new[]
            {
                new ProcessName(4, "Visual Studio"),
                new ProcessName(5, "Outlook"),
            }.ToObservable();
            var pNamesStream3 = processNamesObs1.Select(e => StreamEvent.CreateInterval(0, 10000, e)).
                    Concat(processNamesObs2.Select(e => StreamEvent.CreateStart(0, e))).
                    Concat(processNamesObs2.Select(e => StreamEvent.CreateEnd(10000, 0, e))).
                    ToStreamable(OnCompletedPolicy.EndOfStream());
            pNamesStream3.ToStreamEventObservable().Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 59: Coalescing Matching Edges");
            pNamesStream3.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges()).Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 60: Input with Punctuations");
            var streamablePunctuations = new[]
            {
                StreamEvent.CreateInterval(0, 1, 1),
                StreamEvent.CreateInterval(3, 4, 2),
                StreamEvent.CreatePunctuation<int>(10),
                StreamEvent.CreatePunctuation<int>(20),
                StreamEvent.CreatePunctuation<int>(30),
                StreamEvent.CreatePunctuation<int>(40),
                StreamEvent.CreateInterval(40, 41, 3)
            }.ToObservable().ToStreamable(OnCompletedPolicy.None(), DisorderPolicy.Drop());
            streamablePunctuations.ToStreamEventObservable().Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 61: Input with Generated Punctuations After Every Event");
            var streamableIthEventPunctuations = new[]
            {
                StreamEvent.CreateInterval(0, 1, 1),
                StreamEvent.CreateInterval(3, 4, 2),
                StreamEvent.CreateInterval(40, 41, 3)
            }.ToObservable().ToStreamable(OnCompletedPolicy.None(), DisorderPolicy.Drop(), 
                                          PeriodicPunctuationPolicy.Count(1));
            streamableIthEventPunctuations.ToStreamEventObservable().Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 62: Input with Generated Punctuations After 10 Tick Periods");
            var streamableTimePeriodEventPunctuations = new[]
            {
                StreamEvent.CreateInterval(0, 1, 1),
                StreamEvent.CreateInterval(10, 4, 2),
                StreamEvent.CreateInterval(19, 4, 3),
                StreamEvent.CreateInterval(40, 41, 4)
            }.ToObservable().ToStreamable(OnCompletedPolicy.None(), DisorderPolicy.Drop(), 
                                          PeriodicPunctuationPolicy.Time(10));
            streamableTimePeriodEventPunctuations.ToStreamEventObservable().Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 63: Query with No Output");
            var IncompleteOutputQuery1 = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2)
            }.ToObservable().ToStreamable(OnCompletedPolicy.None()).Count().ToStreamEventObservable();
            IncompleteOutputQuery1.Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 64: Query with Output up to Time 1");
            var IncompleteOutputQuery2 = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2),
                StreamEvent.CreatePunctuation<int>(1)
            }.ToObservable().ToStreamable(OnCompletedPolicy.None()).Count().ToStreamEventObservable();
            IncompleteOutputQuery2.Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 65: Alternate Query with Output up to Time 1");
            var IncompleteOutputQuery3 = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2)
            }.ToObservable().ToStreamable(OnCompletedPolicy.Flush()).Count().ToStreamEventObservable();
            IncompleteOutputQuery3.Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 66: Query with all Output");
            var CompleteOutputQuery1 = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToObservable().ToStreamable(OnCompletedPolicy.None()).Count().ToStreamEventObservable();
            CompleteOutputQuery1.Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            Console.WriteLine("Figure 67: Alternate Query with all Output");
            var CompleteOutputQuery2 = new[]
            {
                StreamEvent.CreateInterval(0, 10, 1),
                StreamEvent.CreateInterval(1, 11, 2)
            }.ToObservable().ToStreamable(OnCompletedPolicy.EndOfStream()).Count().
            ToStreamEventObservable();
            CompleteOutputQuery2.Where(e => e.IsData || e.IsPunctuation).ForEachAsync(e => WriteEvent(e)).Wait();
            Console.WriteLine();

            #endregion

            Console.ReadLine();

        }
    }
}
