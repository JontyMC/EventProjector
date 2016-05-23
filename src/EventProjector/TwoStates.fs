module EventProjector.TwoStates

(*
 * It's (something like) the Free Monad + Interpreter pattern.
 * http://programmers.stackexchange.com/questions/242795/what-is-the-free-monad-interpreter-pattern
 *
 * Strap in for a mega-comment.
 * (I'd recommend reading both this and the Q&A above;
 * reading multiple explanations of the same thing is usually helpful.)
 *
 *
 * We want to structurally enforce some separation between the business logic
 * of doing things to entities in a database and the operational code
 * concerned with the ins-and-outs of talking to the DB.
 * You can use a repository to do this, but that makes batching up database operations difficult:
 * you have to maintain a list of changes to your objects and make sure you flush them when your
 * transaction is over. It's even harder to batch up **loading** of multiple entities -
 * caching can help a bit here but in the worst case you end up making repeated
 * trips to the database to load data when you traverse relationships between entities.
 * These are (some of) the reasons NHibernate is so complicated.
 *
 *
 * Here's an alternative design with a much more functional flavour.
 * Instead of imperatively accessing the database and attempting to optimise things post hoc,
 * we're going to model a transaction script in-memory as a syntax tree.
 * We are then free to interpret that syntax tree however we like by writing a function
 * which recursively traverses the tree using pattern matching and performing a given action.
 * For example, we could evaluate a query against Elastic Search, or compile it to SQL,
 * or run it against an in-memory dictionary (for testing),
 * or print a string representation of it to the console, or...
 *
 *
 * How do we model such a syntax tree? Fundamentally, a transaction script is a
 * **sequence of database requests** to load and save entities.
 *   1. When you're saving an entity, you supply an instance of the entity to save,
 *      and you also supply the next request in the sequence:
 *
 *          Save of Entity * Request<'a>
 *
 *   2. When you're loading an entity, you give an ID to load and the next request
 *      in the sequence, to run after the entity has been loaded.
 *      But the next request in the sequence may depend upon the object that was loaded.
 *      (For example, we might want to load the identity associated with a member.)
 *      So a 'load' request is modelled as an ID with a continuation function to
 *      execute after the entity has been loaded.
 *
 *          Load of Id * (Entity -> Request<'a>)
 *
 *   3. When you've finished talking to the database, you're allowed to
 *      return a value of your choice.
 *      (For example, you might want to return the Member that you just loaded,
 *      or the count of entities that you modified, or whatever.)
 *
 *          Return of 'a
 *
 * This reasoning has led us to a datatype for requests which looks like this.
 *
 *     type Request<'a> = Save of Entity * Request<'a>
 *                      | Load of Id * (Entity -> Request<'a>)
 *                      | Return of 'a
 *
 * A request is a sequence of Load and Save actions; each particular action
 * may depend on the result of actions before it (because of the function inside Load).
 * Requests are always terminated by Return, which is meant to model "wrapping up a value"
 * into the Request type (without executing any actual real-world database requests.)
 *
 * You'll notice that this isn't _exactly_ the same type as what we have below.
 * Firstly, you'll see that Request has two more type parameters, 'State1 and 'State2.
 * This is because there are two types of entity in our system.
 * As a result, there are actually *two* copies of the Load and Save constructors
 * (LoadState1 and LoadState2).
 * Also, it turned out to be much simpler to put IDs separately in the Save request
 * (even though theoretically you could get it from the entity.)
 *
 *
 * At this point, a database request looks kinda Lisp-y:
 *
 *     // update the name of the Identity associated with Member 123
 *     let myRequest = LoadState1 (123, (fun x ->
 *                       LoadState2 (x.IdentityId, (fun y ->
 *                         SaveState2 (y.Id, { y with Name = x.Name },
 *                           Return ()
 *                         )
 *                       ))
 *                     ))
 *
 *
 * The main thing that's missing is a way to sequence actions.
 * Every request is terminated by Return, and there's no way to join
 * two requests together into a larger request.
 * That's accomplished by the Bind() method on ProjectionBuilder below:
 * Bind takes a Request<'a> and a function (f : 'a -> Request<'b>)
 * and joins up the two requests.
 * It does this by recursively walking down the sequence of requests
 * until it finds a Return. Then it pops the Return off the end of the first
 * request and tacks on the result of the function f instead.
 *
 * So now we can write atomic requests and join them together using Bind.
 * For example, loadState1 loads the entity with the given ID and then
 * immediately returns it, terminating the request.
 * Re-writing the above example, note that the explicit call to
 * Return is no longer necessary, and we have replaced constructor calls with Bind().
 *
 *     let loadState1 id = LoadState1 (id, Return)
 *     let loadState2 id = LoadState2 (id, Return)
 *     let saveState1 id a = SaveState1 (id, a, Return(()))
 *     let saveState2 id m = SaveState2 (id, m, Return(()))
 *
 *     let myRequest = Bind(loadState1 123, fun x ->
 *                       Bind(loadState2 x.IdentityId, fun y ->
 *                         saveState2 y.Id { y with Name = x.Name }
 *                       )
 *                     )
 *
 * Here is the last piece of the puzzle.
 * Bind() lets us sequence requests, but it has a little bit of extra magic built in.
 * Defining a type with a method called Bind() enables F#'s Computation Expression syntax.
 * Computation Expressions are syntactic sugar for calls to Bind() which makes
 * functional programs look imperative. We can rewrite the above program using
 * computation expressions and suddenly it doesn't look like Lisp any more.
 *
 *     let myRequest = project {
 *         let! x = loadState1 123
 *         let! y = loadState2 x.IdentityId
 *         do! saveState2 y.Id { y with Name = x.Name }
 *     }
 *
 *
 * As I said, it looks like an imperative program but all it really does is build an expression tree.
 * To prove this to you, here's a dumb example of a way to process an expression tree by pattern matching.
 * All this function does is return the name of the first request in the sequence.
 * 
 *     let firstRequest req = match req with
 *                                  | LoadState1 _ -> "LoadState1"
 *                                  | LoadState2 _ -> "LoadState2"
 *                                  | SaveState1 _ -> "SaveState1"
 *                                  | SaveState2 _ -> "SaveState2"
 *                                  | Return _ -> "Return"
 *
 * In this solution we leverage the "just data"-nature of a Request
 * to attempt to optimise the number of requests we make.
 * We do an initial traversal of the tree to determine which IDs we need to load from Elastic Search,
 * load them all, and then run the request against the data in memory before flushing everything.
 *)

open System.Collections.Generic
open EventProjector.Core
open EventProjector.Logging
open EventProjector.Serialization
open System.Linq

type LoadStates<'State1, 'State2> = Id Set * Id Set -> Async<States<'State1> * States<'State2>>
type SaveStates<'State1, 'State2> = States<'State1> -> States<'State2> -> EventNumbers -> Async<unit>

type StateStore<'State1, 'State2> = {
    LoadCheckpoints: LoadCheckpoints
    Load: LoadStates<'State1, 'State2>
    Save: SaveStates<'State1, 'State2>
}

type Request<'State1, 'State2, 'a> =
    | LoadState1 of Id * ('State1 -> Request<'State1, 'State2, 'a>)
    | LoadState2 of Id * ('State2 -> Request<'State1, 'State2, 'a>)
    | SaveState1 of Id * 'State1 * Request<'State1, 'State2, 'a>
    | SaveState2 of Id * 'State2 * Request<'State1, 'State2, 'a>
    | Return of 'a

let loadState1 id = LoadState1 (id, Return)
let loadState2 id = LoadState2 (id, Return)
let saveState1 id a = SaveState1 (id, a, Return(()))
let saveState2 id m = SaveState2 (id, m, Return(()))

type ProjectionBuilder() =
    member this.Bind(db, f) =
        match db with
        | LoadState1 (id, g) -> LoadState1 (id, fun a -> this.Bind(g a, f))
        | LoadState2 (id, g) -> LoadState2 (id, fun m -> this.Bind(g m, f))
        | SaveState1 (id, a, rest) -> SaveState1 (id, a, this.Bind(rest, f))
        | SaveState2 (id, m, rest) -> SaveState2 (id, m, this.Bind(rest, f))
        | Return x -> f x
    member this.Return(x) = Return x

let project = ProjectionBuilder()

let parseIds (empty1, empty2) =
    let rec parseIdsForRequest =
        function
        | LoadState1 (id, f) ->
            let (ids1:Id Set, ids2:Id Set) = parseIdsForRequest (f empty1)
            (ids1.Add id, ids2)
        | LoadState2 (id, f) ->
            let (ids1, ids2) = parseIdsForRequest (f empty2)
            (ids1, ids2.Add id)
        | SaveState1 (_, _, rest) -> parseIdsForRequest rest
        | SaveState2 (_, _, rest) -> parseIdsForRequest rest
        | Return _ -> (Set.empty, Set.empty)

    let requestsToIds requests =
        requests
        |> List.map parseIdsForRequest
        |> List.fold (fun (ids1Acc, ids2Acc) (ids1, ids2) -> (Set.union ids1 ids1Acc, Set.union ids2 ids2Acc)) (Set.empty, Set.empty)
        
    requestsToIds

let executeBatch states requests =
    let executeRequestUntilIdNotFound acc request =
        let rec reduceRequest (unprocessedRequests, (states1:States<'State1>, states2:States<'State2>)) request = 
            match request with 
            | LoadState1 (id, f) -> 
                let success, state = states1.TryGetValue id
                if success then  reduceRequest (unprocessedRequests, (states1, states2)) (f state)
                else (LoadState1 (id, f) :: unprocessedRequests, (states1, states2))
            | LoadState2 (id, f) ->
                let success, state = states2.TryGetValue id
                if success then reduceRequest (unprocessedRequests, (states1, states2)) (f state)
                else (LoadState2 (id, f) :: unprocessedRequests, (states1, states2))
            | SaveState1 (id, s, r) -> 
                states1.[id] <- s
                reduceRequest (unprocessedRequests, (states1, states2)) r
            | SaveState2 (id, s, r) -> 
                states2.[id] <- s
                reduceRequest (unprocessedRequests, (states1, states2)) r
            | Return x -> (unprocessedRequests, (states1, states2))
        reduceRequest acc request

    List.fold executeRequestUntilIdNotFound ([], states) requests

let setStates (ids1, ids2) (states1, states2) (empty1, empty2) (states1Loaded, states2Loaded) = 
    let setState (states:States<'State>) (statesLoaded:States<'State>) empty id = 
        let loaded, state = statesLoaded.TryGetValue id
        let state = if loaded then state else empty
        states.[id] <- state
    ids1 |> Seq.iter (setState states1 states1Loaded empty1)
    ids2 |> Seq.iter (setState states2 states2Loaded empty2)

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
    execBatches requests (Dictionary<Id, 'State1>() :> States<'State1>, Dictionary<Id, 'State2>() :> States<'State2>)

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
    let! (states1, states2) = execBatches stateStore.Load emptyStates requests
    do! stateStore.Save states1 states2 lastNumberByCat
    batchComplete !count
}

let subscribeProjection projection serializer subscriptions stateStore emptyStates batchSize batchComplete timeout = async {
    let categories = subscriptions |> Seq.map (fun x -> x.Category)
    let! checkpoints = stateStore.LoadCheckpoints categories
    let subscriptions = subscriptions |> Seq.map (fun x -> x.Subscribe checkpoints.[x.Category])
    let execute = execute projection serializer stateStore emptyStates batchComplete
    let subscriber = BatchedEventSubscriber(subscriptions, execute, batchSize, timeout)
    return subscriber.Subscribe()
}