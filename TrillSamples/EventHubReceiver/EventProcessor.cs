namespace EventHubReceiver
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.StreamProcessing;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Event processor for Trill query with state
    /// </summary>
    public class EventProcessor : IEventProcessor
    {
        private Stopwatch _checkpointStopWatch;
        private CloudBlobContainer checkpointContainer;
        private Subject<StreamEvent<long>> input;
        private QueryContainer q;
        private Microsoft.StreamProcessing.Process p;

        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);
        private static readonly string StorageConnectionString = Program.StorageConnectionString;

        /// <summary>
        /// Close processor for partition
        /// </summary>
        /// <param name="context"></param>
        /// <param name="reason"></param>
        /// <returns></returns>
        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine($"Processor Shutting Down. Partition '{context.PartitionId}', Reason: '{reason}'.");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Open processor for partition
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public Task OpenAsync(PartitionContext context)
        {
            Config.ForceRowBasedExecution = true;

            _checkpointStopWatch = new Stopwatch();
            _checkpointStopWatch.Start();

            var _storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var _blobClient = _storageAccount.CreateCloudBlobClient();
            checkpointContainer = _blobClient.GetContainerReference("checkpoints");
            checkpointContainer.CreateIfNotExistsAsync().Wait();

            var blockBlob = checkpointContainer.GetBlockBlobReference(context.Lease.PartitionId + "-" + context.Lease.SequenceNumber);
            if (blockBlob.Exists())
            {
                Console.WriteLine($"Restoring query from EH checkpoint {context.Lease.SequenceNumber}");
                var stream = blockBlob.OpenReadAsync().GetAwaiter().GetResult();
                CreateQuery();
                try
                {
                    p = q.Restore(stream);
                }
                catch
                {
                    Console.WriteLine($"Unable to restore from checkpoint, starting clean");
                    CreateQuery();
                    p = q.Restore();
                }
            }
            else
            {
                Console.WriteLine($"Clean start of query");
                CreateQuery();
                p = q.Restore();
            }

            Console.WriteLine($"SimpleEventProcessor initialized. Partition: '{context.PartitionId}', Lease SeqNo: '{context.Lease.SequenceNumber}'");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Process errors
        /// </summary>
        /// <param name="context"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            Console.WriteLine($"Error on Partition: {context.PartitionId}, Error: {error.Message}");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Process events
        /// </summary>
        /// <param name="context"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            long lastSeq = 0;
            foreach (var eventData in messages)
            {
                var message = BinarySerializer.DeserializeStreamEventLong(eventData.Body.ToArray());
                lastSeq = eventData.SystemProperties.SequenceNumber;
                input.OnNext(message);
            }


            if (_checkpointStopWatch.Elapsed > TimeSpan.FromSeconds(10))
            {
                Console.WriteLine("Taking checkpoint");
                var _storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
                var _blobClient = _storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = _blobClient.GetContainerReference("checkpoints");
                var blockBlob = container.GetBlockBlobReference(context.PartitionId + "-" + lastSeq);
                CloudBlobStream blobStream = blockBlob.OpenWriteAsync().GetAwaiter().GetResult();
                p.Checkpoint(blobStream);
                blobStream.Flush();
                blobStream.Close();

                return context.CheckpointAsync().ContinueWith(t => DeleteOlderCheckpoints(context.PartitionId + "-" + lastSeq));
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Create query and register subscriber
        /// </summary>
        private void CreateQuery()
        {
            q = new QueryContainer();
            input = new Subject<StreamEvent<long>>();
            var inputStream = q.RegisterInput(input, OnCompletedPolicy.EndOfStream(), DisorderPolicy.Drop(), PeriodicPunctuationPolicy.Time(1));
            var query = inputStream.AlterEventDuration(StreamEvent.InfinitySyncTime).Count();
            var async = q.RegisterOutput(query);
            async.Where(e => e.IsStart).ForEachAsync(o => Console.WriteLine($"{o}"));
        }

        /// <summary>
        /// Delete checkpoints other than specified last checkpoint file
        /// </summary>
        /// <param name="checkpointFile"></param>
        /// <returns></returns>
        private Task DeleteOlderCheckpoints(string checkpointFile)
        {
            var _storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var _blobClient = _storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = _blobClient.GetContainerReference("checkpoints");
            foreach (var blob in container.ListBlobs())
            {
                if (((CloudBlockBlob)blob).Name != checkpointFile)
                    ((CloudBlockBlob)blob).Delete();
            }
            _checkpointStopWatch.Restart();
            return Task.CompletedTask;
        }
    }
}