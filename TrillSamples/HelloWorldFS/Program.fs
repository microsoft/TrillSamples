open System
open System.Data.SqlTypes
open System.Linq.Expressions
open System.Reactive.Linq
open Microsoft.StreamProcessing

module Streamable =
    open Microsoft.StreamProcessing

    let ofObservable observable =
        Streamable.ToStreamable(
            observable, 
            DisorderPolicy.Throw(), 
            FlushPolicy.FlushOnPunctuation, 
            PeriodicPunctuationPolicy.None(), 
            OnCompletedPolicy.EndOfStream)
            
    let join mapping left right =
        Streamable.Join(left, right, fun a b -> mapping a b)
        
    let multicast (mapping:IStreamable<'k,'a> -> IStreamable<'k,'b>) source =
        Streamable.Multicast(source, mapping)
        
    let toStreamEventObservable (source:IStreamable<Empty,'a>) =
        Streamable.ToStreamEventObservable(source)
        
    let alterEventLifetime (duration:int64) startTimeSelector source =
        Streamable.AlterEventLifetime(source, (fun x -> startTimeSelector x), duration)
        
    let filter predicate source =
        Streamable.Where(source, fun e -> predicate e)
        
    let map mapping source =
        Streamable.Select(source, fun e -> mapping e)

type SensorRange = { Time: int64; Low: int; High: int }
type SensorReading = { Time: int64; Value: int }

let historicData =
    [ 0; 20; 15; 30; 45; 50; 30; 35; 60; 20 ]
    |> Seq.mapi (fun i v -> { Time = (int64)(i + 1); Value = v } )

let historicStream =
    historicData
    |> Observable.ToObservable
    |> Observable.map (fun r -> StreamEvent.CreateInterval(r.Time, r.Time + 1L, r))
    |> Streamable.ofObservable
    
let createStream isRealTime =
    historicStream
    
let ranges threshold input =
    let above = input |> Streamable.filter (fun s -> s.Value > threshold)
    let below = input |> Streamable.alterEventLifetime 1L ((+) 1L) |> Streamable.filter (fun s -> s.Value < threshold)
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