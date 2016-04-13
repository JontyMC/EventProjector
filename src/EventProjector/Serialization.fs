module EventProjector.Serialization

open Newtonsoft.Json
open EventProjector.Core
open EventProjector.Logging
open Microsoft.FSharp.Reflection
open System
open System.IO
open System.Collections.Generic
open System.Reflection
open Newtonsoft.Json.Linq

type IdConverter() =
    inherit JsonConverter()
    let idType = typeof<Id>

    override x.CanConvert(t) =
        t = idType

    override x.WriteJson(writer, value, serializer) =
        let value =
            let _, fields = FSharpValue.GetUnionFields(value, value.GetType())
            fields.[0]
        serializer.Serialize(writer, value)

    override x.ReadJson(reader, t, _, serializer) =       
        let value = serializer.Deserialize(reader, typedefof<string>)
        if value = null
        then null
        else
            let cases = FSharpType.GetUnionCases(t)
            FSharpValue.MakeUnion(cases.[0], [|value|])

type OptionConverter() =
    inherit JsonConverter()
    
    override x.CanConvert(t) = 
        t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>>

    override x.WriteJson(writer, value, serializer) =
        if value = null then ()
        else 
            let _,fields = FSharpValue.GetUnionFields(value, value.GetType())
            serializer.Serialize(writer, fields.[0])

    override x.ReadJson(reader, t, _, serializer) =
        let genericArgs = t.GetGenericArguments()
        let firstGenericType = genericArgs.[0]
        let innerType = 
            if firstGenericType.IsValueType then (typedefof<Nullable<_>>).MakeGenericType([|firstGenericType|])
            else firstGenericType
        //TODO: cache unioncases and use precomputedunionconstructor
        let value = serializer.Deserialize(reader, innerType)
        let cases = FSharpType.GetUnionCases(t)
        if value = null
        then FSharpValue.MakeUnion(cases.[0], [||])
        else FSharpValue.MakeUnion(cases.[1], [|value|])

let defaultSerializer = JsonSerializer()
defaultSerializer.Converters.Add(OptionConverter())
defaultSerializer.Converters.Add(IdConverter())
defaultSerializer.StringEscapeHandling <- StringEscapeHandling.EscapeNonAscii

let serialize (serializer:JsonSerializer) doc =
    use ms = new MemoryStream()
    (
        use jsonWriter = new JsonTextWriter(new StreamWriter(ms))
        serializer.Serialize(jsonWriter, doc)
    )
    ms.ToArray()

let rec deserialize<'a> (serializer:JsonSerializer) (data:byte[]) =
    let docType = typeof<'a>
    use stream = new MemoryStream(data)
    use reader = new StreamReader(stream)
    use jsonReader = new JsonTextReader(reader)
    let obj = serializer.Deserialize(jsonReader, docType)
    obj :?> 'a

type UnionField = { Name:string; ValueType:Type }
type UnionCase = {
    Name:string;
    Tag:int;
    Fields:IDictionary<string, UnionField>;
    Constructor:obj[] -> obj;
    Reader:obj -> obj[]
}
type Union = {
    CaseByName:string -> Option<UnionCase>;
    CaseFromValue:obj -> UnionCase
}

let createUnion unionType =
    let getUnionCaseTag case =
        let tagProperty = typeof<UnionCaseInfo>.GetProperty("Tag")
        tagProperty.GetValue(case) :?> int
    let mapField (field:PropertyInfo) = { Name = field.Name; ValueType = field.PropertyType }
    let mapCase (case:UnionCaseInfo) =
        let tag = getUnionCaseTag case
        let precomputedCons = FSharpValue.PreComputeUnionConstructor case
        let unionCons fields = precomputedCons fields
        let fields = case.GetFields() |> Seq.map mapField
        let fieldsDict = fields |> Seq.map (fun x -> (x.Name, x)) |> dict
        let precomputedReader = FSharpValue.PreComputeUnionReader case
        { Name = case.Name; Tag = tag; Fields = fieldsDict; Constructor = unionCons; Reader = precomputedReader }
    let cases = FSharpType.GetUnionCases unionType |> Seq.map mapCase
    let casesByName = cases |> Seq.map (fun x -> (x.Name, x)) |> dict
    let caseByName name =
        let success, case = casesByName.TryGetValue name
        if success then Some case else None
    let casesByTag = cases |> Seq.map (fun x -> (x.Tag, x)) |> dict
    let tagReader = FSharpValue.PreComputeUnionTagReader unionType
    let caseFromValue caseValue =
        let tag = tagReader caseValue
        casesByTag.[tag]
    { CaseByName = caseByName; CaseFromValue = caseFromValue }

let serializeNamedFieldUnionCase (serializer:JsonSerializer) (unionCase:UnionCase) value =
    let fieldValues = unionCase.Reader value
    let writeField (writer:JsonTextWriter) (fieldValue:obj, field:UnionField) =
        writer.WritePropertyName(field.Name)
        serializer.Serialize(writer, fieldValue)  
    use stream = new MemoryStream()
    use writer = new StreamWriter(stream)
    use jsonWriter = new JsonTextWriter(writer)
    jsonWriter.WriteStartObject() |> ignore
    unionCase.Fields.Values
        |> Seq.zip fieldValues
        |> Seq.iter (writeField jsonWriter)
    jsonWriter.WriteEndObject()
    writer.Flush()
    stream.ToArray()

let serializeNamedFieldUnion serializer (union:Union) value =
    let unionCase = union.CaseFromValue value
    serializeNamedFieldUnionCase serializer unionCase value

let rec deserializeNamedFieldUnion serializer (unionCase:UnionCase) (data:byte[]) =
    use stream = new MemoryStream(data)
    use reader = new StreamReader(stream)
    use jsonReader = new JsonTextReader(reader)
    let fields =
        let acc = List<obj>()
        let parseField () =
            let name = string jsonReader.Value
            let success, unionField = unionCase.Fields.TryGetValue name
            if success then
                let fieldName = jsonReader.Value
                jsonReader.Read() |> ignore
                let fieldValue = JToken.ReadFrom jsonReader
                let obj = fieldValue.ToObject(unionField.ValueType, serializer)
                acc.Add obj
            else log.Info ("Could not serialize case '" + unionCase.Name + "'. No field named '" + name + "'")
        let rec readJson() =
            jsonReader.Read() |> ignore
            match jsonReader.TokenType with
            | JsonToken.EndObject -> ()
            | JsonToken.PropertyName -> parseField (); readJson()
            | _ -> readJson()
        readJson()
        acc.ToArray()
    unionCase.Constructor fields

let eventToCategoryUnion<'CategoryUnion> serializer =
    let unionType = typeof<'CategoryUnion>
    if not (FSharpType.IsUnion unionType)
    then failwith "Category type must be a discriminated union"
    let mapCase (case:UnionCaseInfo) =
        let categoryFields = case.GetFields()
        if categoryFields.Length <> 1
        then failwith "Each Category discriminated union case must define exactly one field for the event"
        let eventType = categoryFields.[0].PropertyType
        let eventUnion = createUnion eventType
        let precomputedCons = FSharpValue.PreComputeUnionConstructor case
        let unionCons event =
            let eventCase = eventUnion.CaseByName event.Type
            if not eventCase.IsSome
            then
                log.Debug("No handler for event type '" + event.Type + "'")
                None
            else
                let event = deserializeNamedFieldUnion serializer eventCase.Value event.Data
                Some (precomputedCons [| event |] :?> 'CategoryUnion)
        (Category case.Name, unionCons)
    let eventConsByCategory = FSharpType.GetUnionCases unionType |> Seq.map mapCase |> dict
    let eventToCategoryEvent (event:Event) =
        let success, eventCons = eventConsByCategory.TryGetValue event.Category
        if not success then failwith ("Category '" + string event.Category + "' not defined in type '" + unionType.Name + "'")
        eventCons event
    eventToCategoryEvent