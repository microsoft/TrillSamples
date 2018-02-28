using Microsoft.StreamProcessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace CheckpointExample
{
    class Program
    {
        static void Main(string[] args)
        {
            // Subjects to feed pre- and post-checkpoint data to the query
            var preCheckpointSubject = new Subject<StreamEvent<ValueTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<ValueTuple<int, int>>>();

            // Outputs of queries with and without checkpointing
            var outputListWithCheckpoint = new List<ValueTuple<int, ulong>>();
            var outputListWithoutCheckpoint = new List<ValueTuple<int, ulong>>();

            // Containers are an abstraction to hold queries and are the unit of checkpointing
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            
            // Query state is written to and read from a .NET stream
            Stream state = new MemoryStream();

            // Input data: first half of the dataset before a checkpoint is taken
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new ValueTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Input data: second half of the dataset after a checkpoint is taken
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new ValueTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // For comparison, we run the same query directly on the full dataset
            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new ValueTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Run with first half of the dataset, then take a checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, OnCompletedPolicy.EndOfStream());
            var query1 = input1.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new ValueTuple<int, ulong> { Item1 = g.Key, Item2 = c });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            preCheckpointSubject.OnNext(StreamEvent.CreatePunctuation<ValueTuple<int, int>>(9999));
            pipe1.Checkpoint(state);

            // Seek to the beginning of the stream that represents checkpointed state
            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore the state from the saved checkpoint, and feed the second half of the dataset
            var input2 = container2.RegisterInput(postCheckpointSubject, OnCompletedPolicy.EndOfStream());
            var query2 = input2.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new ValueTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();

            // Sort the payloads in the query result
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: For comparison, run the query directly on the entire dataset without any checkpoint/restore
            var input3 = container3.RegisterInput(fullData, OnCompletedPolicy.EndOfStream());
            var query3 = input3.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new ValueTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null); // The parameter of "null" to restore causes it to run from scratch
            outputAsync3.Wait();

            // Sort the payloads in the query result
            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Perform a comparison of the checkpoint/restore query result and the result of the original query run directly on the entire dataset
            if (outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint))
            {
                Console.WriteLine("SUCCESS: Output of query with checkpoint/restore matched output of uninterrupted query");
            }
            else
            {
                Console.WriteLine("ERROR: Output of query with checkpoint/restore did not match the output of uninterrupted query");
            }
            Console.ReadLine();
        }
    }
}
