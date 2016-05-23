module EventProjector.Core

open System.Diagnostics
open System.Collections.Generic
open System.Collections.Concurrent
open EventProjector.Logging
open EventProjector.Async
open System.Threading
open Microsoft.FSharp.Reflection

type Id = Id of string
    with override x.ToString() =
            let (Id id) = x
            id

type Category = Category of string
    with override x.ToString() =
            let (Category category) = x
            category

type EventNumber = uint32
type Checkpoint = | Start | EventNumber of EventNumber          

type Event = { Category:Category; Id:Id; Number:EventNumber; Type:string; Data:byte[] }

type Subscription = {
    Category: Category
    Subscribe: Checkpoint -> (Event -> unit) -> (unit -> unit)
}

type LoadCheckpoints = seq<Category> -> Async<IDictionary<Category, Checkpoint>>
type EventNumbers = IDictionary<Category, EventNumber>
type States<'State> = IDictionary<Id, 'State>

type BatchedEventSubscriber(subscriptions:seq<(Event -> unit) -> unit -> unit>, project, batchSize:int, timeOut:int) =
    let eventQueue = new BlockingCollection<Event>(new ConcurrentQueue<Event>(), batchSize)
    let batchExecuting = ref 0

    member this.Subscribe() =
        let executeBatch() = 
            let rec loopUntilEmpty (events:List<Event>) = async {
                let rec loopTryTake() = async {
                    let gotEvent, event = eventQueue.TryTake()
                    if gotEvent then events.Add event
                    if (not gotEvent || events.Count >= batchSize) && events.Count > 0
                    then do! project (events |> List.ofSeq)
                    else do! loopTryTake()
                }
                do! loopTryTake()
                Interlocked.Exchange(batchExecuting, 0) |> ignore
                if eventQueue.Count > 0 && Interlocked.CompareExchange(batchExecuting, 1, 0) = 0
                then do! loopUntilEmpty (List<Event>())
            }
            loopUntilEmpty (List<Event>())
        let eventAppeared event =
            let executeBatch () =   
                executeBatch() |> Async.Catch |> Async.RunSynchronously |> Async.Rethrow
            eventQueue.Add event
            if Interlocked.CompareExchange(batchExecuting, 1, 0) = 0
            then ThreadPool.QueueUserWorkItem(fun _ -> executeBatch()) |> ignore
        let unsubscribes = subscriptions |> Seq.map (fun x -> x eventAppeared) |> Seq.toArray
        let handleRemainingEvents () =
            let timeOut = int64 (timeOut * 1000)
            Seq.iter (fun x -> x()) unsubscribes
            let stopwatch = Stopwatch.StartNew()
            while stopwatch.ElapsedMilliseconds < timeOut && (!batchExecuting = 1 || eventQueue.Count > 0) do ()
        handleRemainingEvents
