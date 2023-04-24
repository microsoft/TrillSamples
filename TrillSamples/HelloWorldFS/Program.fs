open System
open System.Data.SqlTypes
open System.Linq.Expressions
open System.Reactive.Linq
open Microsoft.StreamProcessing

open System.Runtime.CompilerServices

[<Extension>]
type Observable with
    [<Extension>]
    static member inline toStreamable(observable: IObservable<StreamEvent<'a>>, 
                                      ?disorderPolicy : DisorderPolicy, 
                                      ?flushPolicy : FlushPolicy, 
                                      ?periodicPunctuationPolicy : PeriodicPunctuationPolicy,
                                      ?onCompletedPolicy : OnCompletedPolicy) =
        let disorderPolicy = DisorderPolicy.Throw() |> defaultArg disorderPolicy
        let flushPolicy = FlushPolicy.FlushOnPunctuation |> defaultArg flushPolicy 
        let periodicPunctuationPolicy = PeriodicPunctuationPolicy.None() |> defaultArg periodicPunctuationPolicy
        let onCompletedPolicy = OnCompletedPolicy.EndOfStream |> defaultArg onCompletedPolicy
        Streamable.ToStreamable(
                    observable, 
                    disorderPolicy, 
                    flushPolicy, 
                    periodicPunctuationPolicy, 
                    onCompletedPolicy)
    
module Streamable =
    open Microsoft.StreamProcessing
            
    let join map left right =
        Streamable.Join(left, right, fun a b -> map a b)
        
    let keyJoin map left right =
        Streamable.Join(fst left, fst right, (fun a -> a |> snd left), (fun b -> b |> snd right), (fun a b -> map a b))
        
    let hintedKeyJoin options map left right =
        Streamable.Join(fst left, fst right, (fun a -> a |> snd left), (fun b -> b |> snd right), (fun a b -> map a b), options)
        
    let multicast (map:IStreamable<'TKey,'TPayload> -> IStreamable<'TKey,'TResult>) source =
        Streamable.Multicast(source, map)
        
    let multicastMerge (map:IStreamable<'k,'a> -> IStreamable<'k,'b> -> IStreamable<'k,'c>) sourceLeft sourceRight =
        Streamable.Multicast(sourceLeft, sourceRight, fun a b -> map a b)
        
    let multicastCount (count:int) source : IStreamable<'TKey,'TPayload>[] =
        Streamable.Multicast(source, count)
        
    let toStreamEventObservable (source:IStreamable<Empty,'TPayload>) =
        Streamable.ToStreamEventObservable(source)
        
    let toReshapedStreamEventObservable reshapingPolicy (source:IStreamable<Empty,'TPayload>) =
        Streamable.ToStreamEventObservable(source, reshapingPolicy)
        
    let toPartitionedStreamEventObservable (source:IStreamable<PartitionKey<'TKey>,'TPayload>) =
        Streamable.ToStreamEventObservable(source)
        
    let toReshapedPartitionedStreamEventObservable reshapingPolicy (source:IStreamable<PartitionKey<'TKey>,'TPayload>) =
        Streamable.ToStreamEventObservable(source, reshapingPolicy)
        
    let setEventLifetime startTimeSelector (duration:int64) source =
        Streamable.AlterEventLifetime(source, (fun x -> startTimeSelector x), duration)
        
    let mapEventLifetime startTimeSelector (map:int64 -> int64) source =
        Streamable.AlterEventLifetime(source, (fun x -> startTimeSelector x), fun duration -> map duration)
        
    let mergeEventLifetime startTimeSelector (merge:int64 -> int64 -> int64) source =
        Streamable.AlterEventLifetime(source, (fun x -> startTimeSelector x), fun a b -> merge a b)
        
    let mergePartitionedEventLifetime startTimeSelector (merge:'TPartition -> int64 -> int64 -> int64) source : IStreamable<PartitionKey<'TPartition>,'TPayload> =
        Streamable.AlterEventLifetime(source, (fun p x -> startTimeSelector p x), fun a b -> merge a b)
        
    let filter predicate source : IStreamable<'TKey,'TPayload> =
        Streamable.Where(source, fun e -> predicate e)
        
    let map (map:'TPayload->'TResult) source : IStreamable<'TKey,'TResult>=
        Streamable.Select(source, fun e -> map e)

    let bind (map:'TLeft -> 'TRight -> 'TResult) (left:IStreamable<'TKey,'TLeft>) (right:Empty -> IStreamable<'TKey,'TRight>) =
        Streamable.SelectMany(left, right, fun a b -> map a b)

    let groupBind (groupDefinition:IMapDefinition<'TOuterKey,'TPayload,'TPayload,'TInnerKey,'TResult>) apply (resultSelector:GroupSelectorInput<'TInnerKey> -> 'TBind -> 'TOutput) =
        Streamable.SelectMany(groupDefinition, apply, fun i b -> resultSelector i b)

type SensorRange = { Time: int64; Low: int; High: int }
type SensorReading = { Time: int64; Value: int }

let historicData =
    [ 0; 20; 15; 30; 45; 50; 30; 35; 60; 20 ]
    |> Seq.mapi (fun i v -> { Time = (int64)(i + 1); Value = v } )

let historicStream =
    historicData
    |> Observable.ToObservable
    |> Observable.map (fun r -> StreamEvent.CreateInterval(r.Time, r.Time + 1L, r))
    |> Observable.toStreamable
    
let createStream isRealTime =
    historicStream
    
let ranges threshold input =
    let above = input |> Streamable.filter (fun s -> s.Value > threshold)
    let below = input |> Streamable.setEventLifetime ((+) 1L) 1L |> Streamable.filter (fun s -> s.Value < threshold)
    let (|><|) = Streamable.join (fun a b -> { Time = a.Time; Low = b.Value; High = a.Value })
    below |><| above

[<EntryPoint>]
let main argv =
    let ranges =
        createStream false 
        |> Streamable.multicast (ranges 42)
        |> Streamable.toStreamEventObservable
    Observable.ForEachAsync(ranges, fun e -> printfn "%A" e).Wait()
    printfn "Done. Press ENTER to terminate"
    Console.ReadLine() |> ignore
    0