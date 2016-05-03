module EventProjector.ThreeStates

open System.Collections.Generic
open EventProjector.Core
open EventProjector.Logging
open EventProjector.Serialization
open System.Linq

type LoadStates<'State1, 'State2, 'State3> = Id Set * Id Set * Id Set -> Async<States<'State1> * States<'State2> * States<'State3>>
type SaveStates<'State1, 'State2, 'State3> = States<'State1> -> States<'State2> -> States<'State3> -> EventNumbers -> Async<unit>

type StateStore<'State1, 'State2, 'State3> = {
    LoadCheckpoints: LoadCheckpoints
    Load: LoadStates<'State1, 'State2, 'State3>
    Save: SaveStates<'State1, 'State2, 'State3>
}

type Request<'State1, 'State2, 'State3, 'a> =
    | LoadState1 of Id * ('State1 -> Request<'State1, 'State2, 'State3, 'a>)
    | LoadState2 of Id * ('State2 -> Request<'State1, 'State2, 'State3, 'a>)
    | LoadState3 of Id * ('State3 -> Request<'State1, 'State2, 'State3, 'a>)
    | SaveState1 of Id * 'State1 * Request<'State1, 'State2, 'State3, 'a>
    | SaveState2 of Id * 'State2 * Request<'State1, 'State2, 'State3, 'a>
    | SaveState3 of Id * 'State3 * Request<'State1, 'State2, 'State3, 'a>
    | Return of 'a

let loadState1 id = LoadState1 (id, Return)
let loadState2 id = LoadState2 (id, Return)
let loadState3 id = LoadState3 (id, Return)
let saveState1 id a = SaveState1 (id, a, Return(()))
let saveState2 id m = SaveState2 (id, m, Return(()))
let saveState3 id m = SaveState3 (id, m, Return(()))

type ProjectionBuilder() =
    member this.Bind(db, f) =
        match db with
        | LoadState1 (id, g) -> LoadState1 (id, fun a -> this.Bind(g a, f))
        | LoadState2 (id, g) -> LoadState2 (id, fun m -> this.Bind(g m, f))
        | LoadState3 (id, g) -> LoadState3 (id, fun m -> this.Bind(g m, f))
        | SaveState1 (id, a, rest) -> SaveState1 (id, a, this.Bind(rest, f))
        | SaveState2 (id, m, rest) -> SaveState2 (id, m, this.Bind(rest, f))
        | SaveState3 (id, m, rest) -> SaveState3 (id, m, this.Bind(rest, f))
        | Return x -> f x
    member this.Return(x) = Return x

let project = ProjectionBuilder()

let parseIds (empty1, empty2, empty3) =
    let rec parseIdsForRequest =
        function
        | LoadState1 (id, f) ->
            let (ids1:Id Set, ids2:Id Set, ids3:Id Set) = parseIdsForRequest (f empty1)
            (ids1.Add id, ids2, ids3)
        | LoadState2 (id, f) ->
            let (ids1, ids2, ids3) = parseIdsForRequest (f empty2)
            (ids1, ids2.Add id, ids3)
        | LoadState3 (id, f) ->
            let (ids1, ids2, ids3) = parseIdsForRequest (f empty3)
            (ids1, ids2, ids3.Add id)
        | SaveState1 (_, _, rest) -> parseIdsForRequest rest
        | SaveState2 (_, _, rest) -> parseIdsForRequest rest
        | SaveState3 (_, _, rest) -> parseIdsForRequest rest
        | Return _ -> (Set.empty, Set.empty, Set.empty)

    let requestsToIds requests =
        requests
        |> List.map parseIdsForRequest
        |> List.fold (fun (ids1Acc, ids2Acc, ids3Acc) (ids1, ids2, ids3) -> (Set.union ids1 ids1Acc, Set.union ids2 ids2Acc, Set.union ids3 ids3Acc)) (Set.empty, Set.empty, Set.empty)
        
    requestsToIds

let executeBatch states requests =
    let executeRequestUntilIdNotFound acc request =
        let rec reduceRequest (unprocessedRequests, (states1:States<'State1>, states2:States<'State2>, states3:States<'State3>)) request = 
            match request with 
            | LoadState1 (id, f) -> 
                let success, state = states1.TryGetValue id
                if success then  reduceRequest (unprocessedRequests, (states1, states2, states3)) (f state)
                else (LoadState1 (id, f) :: unprocessedRequests, (states1, states2, states3))
            | LoadState2 (id, f) ->
                let success, state = states2.TryGetValue id
                if success then reduceRequest (unprocessedRequests, (states1, states2, states3)) (f state)
                else (LoadState2 (id, f) :: unprocessedRequests, (states1, states2, states3))
            | LoadState3 (id, f) ->
                let success, state = states3.TryGetValue id
                if success then reduceRequest (unprocessedRequests, (states1, states2, states3)) (f state)
                else (LoadState3 (id, f) :: unprocessedRequests, (states1, states2, states3))
            | SaveState1 (id, s, r) -> 
                states1.[id] <- s
                reduceRequest (unprocessedRequests, (states1, states2, states3)) r
            | SaveState2 (id, s, r) -> 
                states2.[id] <- s
                reduceRequest (unprocessedRequests, (states1, states2, states3)) r
            | SaveState3 (id, s, r) -> 
                states3.[id] <- s
                reduceRequest (unprocessedRequests, (states1, states2, states3)) r
            | Return x -> (unprocessedRequests, (states1, states2, states3))
        reduceRequest acc request

    List.fold executeRequestUntilIdNotFound ([], states) requests

let setStates (ids1, ids2, ids3) (states1, states2, states3) (empty1, empty2, empty3) (states1Loaded, states2Loaded, states3Loaded) = 
    let setState (states:States<'State>) (statesLoaded:States<'State>) empty id = 
        let loaded, state = statesLoaded.TryGetValue id
        let state = if loaded then state else empty
        states.[id] <- state
    ids1 |> Seq.iter (setState states1 states1Loaded empty1)
    ids2 |> Seq.iter (setState states2 states2Loaded empty2)
    ids3 |> Seq.iter (setState states3 states3Loaded empty3)

let execBatches load emptyStates requests =
    let rec execBatches requests states = async {
        let ids = parseIds emptyStates requests
        let! loadedStates = load ids
        setStates ids states emptyStates loadedStates
        let requests1, states = executeBatch states requests
        if requests1.IsEmpty
        then return states
        else return! execBatches requests1 states
    }
    execBatches requests (Dictionary<Id, 'State1>() :> States<'State1>, Dictionary<Id, 'State2>() :> States<'State2>, Dictionary<Id, 'State3>() :> States<'State3>)

let execute projection serializer stateStore emptyStates batchComplete (events:seq<Event>) = async {
    let logCategoryEvents category (first, last) =
        log.Info("Projecting from " + string first + " to " + string last + " for category " + string category)
    let eventToCategoryUnion = eventToCategoryUnion<'Categories> serializer
    let eventNumbersByCat = Dictionary<Category, EventNumber * EventNumber>() :> IDictionary<Category, EventNumber * EventNumber>
    let count = ref 0
    let addFirstLast (event:Event) =
        let success, numbers = eventNumbersByCat.TryGetValue event.Category
        if success then
            let (first, _) = numbers
            eventNumbersByCat.[event.Category] <- (first, event.Number)
        else
            eventNumbersByCat.[event.Category] <- (event.Number, event.Number)
    let toCategoryUnion (event:Event) =
        count := !count + 1
        addFirstLast event
        (event.Id, eventToCategoryUnion event)
    let requests =
        events |> Seq.map toCategoryUnion |> Seq.filter (fun (_, x) -> x.IsSome) |> Seq.map (fun (x, y) -> (x, y.Value))
        |> Seq.map (fun (id, event) -> projection id event) |> Seq.toList
    eventNumbersByCat |> Seq.iter (fun x -> logCategoryEvents x.Key x.Value)
    let lastNumberByCat = eventNumbersByCat |> Seq.map (fun (KeyValue(x, (_, y))) -> (x, y)) |> dict
    let! (states1, states2, states3) = execBatches stateStore.Load emptyStates requests
    do! stateStore.Save states1 states2 states3 lastNumberByCat
    batchComplete !count
}

let subscribeProjection projection serializer subscriptions stateStore emptyStates batchSize batchComplete = async {
    let categories = subscriptions |> Seq.map (fun x -> x.Category)
    let! checkpoints = stateStore.LoadCheckpoints categories
    let subscriptions = subscriptions |> Seq.map (fun x -> x.Subscribe checkpoints.[x.Category])
    let execute = execute projection serializer stateStore emptyStates batchComplete
    let subscriber = BatchedEventSubscriber(subscriptions, execute, batchSize)
    return subscriber.Subscribe()
}