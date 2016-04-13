module EventProjector.Logging

open System

type ILog =
    abstract member Debug: string-> unit
    abstract member Info: string -> unit
    abstract member Info: string * exn-> unit
    abstract member Warn: string * exn -> unit
    abstract member Error: string * exn -> unit

type NullLogger() =
    interface ILog with
        member this.Debug message = ()
        member this.Info message = ()
        member this.Info (message, exn) = ()
        member this.Warn (message, exn) = ()
        member this.Error (message, exn) = ()

let mutable log = NullLogger() :> ILog