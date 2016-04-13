module EventProjector.Async

open System
open System.Threading
open System.Threading.Tasks
open System.Runtime.ExceptionServices

type Async with
    static member AwaitPlainTask (t:System.Threading.Tasks.Task) =
        let flattenExns (e:AggregateException) = e.Flatten().InnerExceptions |> Seq.item 0
        let raise (e:#exn) = Async.FromContinuations(fun (_, econt, _) -> econt e)
        let rewrapAsyncExn (it:Async<unit>) =
            async { try do! it with :? AggregateException as ae -> do! (raise <| flattenExns ae) }
        let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.None)
        t.ContinueWith((fun t' ->
            if t.IsFaulted then tcs.SetException(t.Exception |> flattenExns)
            elif t.IsCanceled then tcs.SetCanceled ()
            else tcs.SetResult(())), TaskContinuationOptions.ExecuteSynchronously)
        |> ignore
        tcs.Task |> Async.AwaitTask |> rewrapAsyncExn
    // Unfortunately async errors don't give a useful stacktrace, this is being fixed in 4.0
    // http://stackoverflow.com/questions/18192328/how-to-get-a-useful-stacktrace-when-testing-f-async-workflows
    static member Rethrow x =
        match x with 
        | Choice1Of2 x -> x
        | Choice2Of2 ex ->
            let t = ex
            ExceptionDispatchInfo.Capture(ex).Throw()
            failwith "nothing to return, but will never get here"