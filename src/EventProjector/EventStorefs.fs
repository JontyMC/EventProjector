module EventProjector.EventStore

open System
open EventStore.ClientAPI
open EventProjector.Core
open EventProjector.Logging

let parseStreamId (streamId:string) =
    let streamArgs = streamId.Split [|'-'|]
    let category = Category streamArgs.[0] 
    let id = Id streamArgs.[1]
    (category, id)

let recordedEventToEvent (event:RecordedEvent) (eventNumber:int) =
    let (category, id) = parseStreamId event.EventStreamId
    let eventNumber = uint32 eventNumber
    { Category = category; Id = id; Number = eventNumber; Type = event.EventType; Data = event.Data }

let resolvedEventToEvent (event:ResolvedEvent) = recordedEventToEvent event.Event event.OriginalEventNumber

let toCategoryStreamId category =
    let (Category category) = category
    "$ce-" + category

let eventStoreSubscription (conn:IEventStoreConnection) category batchSize stopTimeOut =
    let categoryStreamId = toCategoryStreamId category
    let subscribe lastCheckpoint eventAppeared =
        let eventNumber =
            match lastCheckpoint with
            | Start -> "the start"
            | EventNumber x -> " event " + string x
        log.Info("Subscribing to category " + string category + " from " + eventNumber)
        let lastCheckpoint =
            match lastCheckpoint with
            | Start -> Nullable<int>()
            | EventNumber n -> Nullable<int>(int n)
        let eventAppeared = Action<EventStoreCatchUpSubscription, ResolvedEvent>(fun _ x ->
            if x.IsResolved && x.Event.EventType <> "$metadata"
            then eventAppeared (resolvedEventToEvent x)
        )
        let liveProcessingStarted = Action<EventStoreCatchUpSubscription>(fun _ -> log.Info("Caught up with stored events for category " + string category))
        let subscription = conn.SubscribeToStreamFrom(categoryStreamId, lastCheckpoint, true, eventAppeared, liveProcessingStarted, (fun x y z -> ()), readBatchSize = batchSize)
        if subscription.LastProcessedEventNumber = -1
        then log.Info("Stream '" + categoryStreamId + "' does not exist")
        fun () -> subscription.Stop stopTimeOut
    { Category = category; Subscribe = subscribe }