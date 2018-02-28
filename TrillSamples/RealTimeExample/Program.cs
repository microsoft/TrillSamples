using System;
using System.Linq;
using System.Reactive.Linq;

using Microsoft.StreamProcessing;

namespace RealTimeExample
{
    class Program
    {

        static void Main(string[] args)
        {
            // Poll performance counter 4 times a second
            TimeSpan pollingInterval = TimeSpan.FromSeconds(0.25);

            // Take the total processor utilization performance counter
            string categoryName = "Processor";
            string counterName = "% Processor Time";
            string instanceName = "_Total";

            // Create an observable that feeds the performance counter periodically
            IObservable<PerformanceCounterSample> source = new PerformanceCounterObservable(categoryName, counterName, instanceName, pollingInterval);

            // Load the observable as a stream in Trill
            var inputStream =
                source.Select(e => StreamEvent.CreateStart(e.StartTime.Ticks, e)) // Create an IObservable of StreamEvent<>
                .ToStreamable(OnCompletedPolicy.Flush(), PeriodicPunctuationPolicy.Count(4)); // Create a streamable with a punctuation every 4 events

            // The query
            long windowSize = TimeSpan.FromSeconds(2).Ticks;
            var query = inputStream.AlterEventDuration(windowSize).Average(e => e.Value);

            // Egress results and write to console
            query.ToStreamEventObservable().ForEachAsync(e => WriteEvent(e)).Wait();
        }


        static void WriteEvent<T>(StreamEvent<T> e)
        {
            if (e.IsInterval)
            {
                Console.WriteLine(
                    "Event Kind=Interval\tStart Time={0}\tEnd Time={1}\tPayload={2}",
                    e.StartTime, e.EndTime, e.Payload);
            }
            else if (e.IsStart)
            {
                Console.WriteLine(
                    "Event Kind=Start\tStart Time={0}\tEnd Time=????\tPayload={1}",
                    e.StartTime, e.Payload);
            }
            else if (e.IsEnd)
            {
                Console.WriteLine(
                    "Event Kind=End\t\tStart Time={0}\tEnd Time={1}\tPayload={2}",
                    e.StartTime, e.EndTime, e.Payload);
            }
            else // Is a punctuation
            {
                Console.WriteLine(
                  "Event Kind=Punctuation\tSync Time={0}",
                  e.SyncTime);
            }
        }
    }
}
