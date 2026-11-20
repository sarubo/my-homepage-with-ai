namespace FileShare

open System
open System.Collections.Generic
open System.IO
open System.IO.Pipelines
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks

[<CLIMutable>]
type StoredFileInfo =
    { Id: Guid
      Name: string
      SizeBytes: int64
      UploadedAtUtc: DateTimeOffset }

[<CLIMutable>]
type DeleteResponse = { Deleted: int }

[<CLIMutable>]
type DeleteRequest = { Ids: Guid array }

type StoreError =
    | Busy
    | TooLarge

type private ChunkedContent =
    { Chunks: byte array array
      Length: int64 }

type private StoredFile =
    { Info: StoredFileInfo
      Content: ChunkedContent }

type FileDownload = private { Name: string; SizeBytes: int64; Content: ChunkedContent }

module FileDownload =
    let private copyToPipe (content: ChunkedContent) (writer: PipeWriter) (cancellationToken: CancellationToken) =
        task {
            try
                let mutable writing = true
                let mutable chunkIndex = 0

                while writing && chunkIndex < content.Chunks.Length do
                    cancellationToken.ThrowIfCancellationRequested()
                    let! flushResult = writer.WriteAsync(content.Chunks.[chunkIndex].AsMemory(), cancellationToken)

                    if flushResult.IsCanceled then
                        cancellationToken.ThrowIfCancellationRequested()
                        raise (OperationCanceledException())

                    writing <- not flushResult.IsCompleted
                    chunkIndex <- chunkIndex + 1

                do! writer.CompleteAsync()
            with error ->
                try
                    do! writer.CompleteAsync(error)
                with _ -> ()
        }

    /// Creates a Stream adapter over a bounded pipeline. The producer task owns
    /// the PipeWriter; disposing the returned stream completes the PipeReader.
    let toStream (file: FileDownload) (cancellationToken: CancellationToken) : Stream =
        let pipe =
            new Pipe(
                PipeOptions(
                    pauseWriterThreshold = 1024L * 1024L,
                    resumeWriterThreshold = 512L * 1024L,
                    useSynchronizationContext = false
                )
            )

        // The copy routine captures and reports its own errors to the reader by
        // completing the writer with the exception, so this task does not fault.
        copyToPipe file.Content pipe.Writer cancellationToken |> ignore
        pipe.Reader.AsStream()

type private State =
    { Files: Map<string, StoredFile>
      UsedBytes: int64
      ReservedBytes: int64
      Reservations: Map<Guid, int64> }

module private State =
    let empty =
        { Files = Map.empty
          UsedBytes = 0L
          ReservedBytes = 0L
          Reservations = Map.empty }

    let list state =
        state.Files
        |> Map.toSeq
        |> Seq.map (fun (_, file) -> file.Info)
        |> Seq.sortBy (fun file -> file.Name)
        |> Seq.toList

    let tryOpen id state =
        state.Files
        |> Map.toSeq
        |> Seq.tryPick (fun (_, file) ->
            if file.Info.Id = id then
                Some
                    { Name = file.Info.Name
                      SizeBytes = file.Info.SizeBytes
                      Content = file.Content }
            else
                None)

    let reserve uploadId requestedBytes state =
        let available = max 0L (4L * 1024L * 1024L * 1024L - state.UsedBytes - state.ReservedBytes)
        let granted = min requestedBytes available

        if granted <= 0L then
            state, 0L
        else
            let previous = state.Reservations |> Map.tryFind uploadId |> Option.defaultValue 0L

            { state with
                ReservedBytes = state.ReservedBytes + granted
                Reservations = Map.add uploadId (previous + granted) state.Reservations },
            granted

    let release uploadId bytes state =
        let previous = state.Reservations |> Map.tryFind uploadId |> Option.defaultValue 0L
        let released = min previous (max 0L bytes)
        let remaining = previous - released

        { state with
            ReservedBytes = max 0L (state.ReservedBytes - released)
            Reservations =
                if remaining = 0L then Map.remove uploadId state.Reservations
                else Map.add uploadId remaining state.Reservations }

    let commit uploadId file state =
        let reserved = state.Reservations |> Map.tryFind uploadId |> Option.defaultValue 0L
        let previousSize =
            state.Files
            |> Map.tryFind file.Info.Name
            |> Option.map (fun previous -> previous.Content.Length)
            |> Option.defaultValue 0L

        { state with
            Files = Map.add file.Info.Name file state.Files
            UsedBytes = state.UsedBytes - previousSize + file.Content.Length
            ReservedBytes = max 0L (state.ReservedBytes - reserved)
            Reservations = Map.remove uploadId state.Reservations }

    let deleteMany (ids: Guid array) (state: State) =
        if isNull ids then
            state, 0
        else
            let selected = HashSet<Guid>(ids)

            let remaining =
                state.Files
                |> Map.filter (fun _ file -> not (selected.Contains(file.Info.Id)))

            let deleted = state.Files.Count - remaining.Count

            let usedBytes =
                remaining
                |> Map.toSeq
                |> Seq.sumBy (fun (_, file) -> file.Content.Length)

            { state with
                Files = remaining
                UsedBytes = usedBytes },
            deleted

module FileName =
    let tryNormalize (fileName: string) =
        if String.IsNullOrWhiteSpace fileName then
            Error "ファイル名がありません。"
        else
            let name = Path.GetFileName(fileName)

            if String.IsNullOrWhiteSpace name then
                Error "有効なファイル名を指定してください。"
            elif name.Length > 255 then
                Error "ファイル名は255文字以下にしてください。"
            elif name.IndexOfAny([| '\u0000'; '\r'; '\n' |]) >= 0 then
                Error "ファイル名に制御文字は使用できません。"
            else
                Ok name

module FileStore =
    let private ChunkSize = 4 * 1024 * 1024
    let private MaxFileSize = 4L * 1024L * 1024L * 1024L

    type private Message =
        | List of TaskCompletionSource<Result<StoredFileInfo list, StoreError>>
        | TryOpen of Guid * TaskCompletionSource<Result<FileDownload option, StoreError>>
        | Reserve of Guid * int64 * TaskCompletionSource<Result<int64, StoreError>>
        | Release of Guid * int64 * TaskCompletionSource<unit>
        | Commit of Guid * StoredFile * TaskCompletionSource<Result<StoredFileInfo, StoreError>>
        | DeleteMany of Guid array * TaskCompletionSource<Result<int, StoreError>>

    type Handle = private { Channel: Channel<Message> }

    let private newReply<'T> () =
        TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)

    let createHandle () =
        let options =
            BoundedChannelOptions(127,
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait)

        { Channel = Channel.CreateBounded<Message>(options) }

    let start (handle: Handle) =
        let rec run state =
            task {
                let! message = handle.Channel.Reader.ReadAsync()

                match message with
                | List reply ->
                    reply.TrySetResult(Ok(State.list state)) |> ignore
                    return! run state
                | TryOpen (id, reply) ->
                    reply.TrySetResult(Ok(State.tryOpen id state)) |> ignore
                    return! run state
                | Reserve (uploadId, requested, reply) ->
                    let nextState, granted = State.reserve uploadId requested state
                    reply.TrySetResult(Ok granted) |> ignore
                    return! run nextState
                | Release (uploadId, bytes, reply) ->
                    let nextState = State.release uploadId bytes state
                    reply.TrySetResult(()) |> ignore
                    return! run nextState
                | Commit (uploadId, stored, reply) ->
                    let nextState = State.commit uploadId stored state
                    reply.TrySetResult(Ok stored.Info) |> ignore
                    return! run nextState
                | DeleteMany (ids, reply) ->
                    let nextState, deleted = State.deleteMany ids state
                    reply.TrySetResult(Ok deleted) |> ignore
                    return! run nextState
            }

        run State.empty

    let private ask (handle: Handle) tryWrite createMessage =
            task {
                let reply = newReply ()
                let message = createMessage reply

                if not (tryWrite message) then
                    return Error Busy
                else
                    return! reply.Task
            }

    let private askRequired (handle: Handle) (cancellationToken: CancellationToken) createMessage =
            task {
                let reply = newReply ()
                do! handle.Channel.Writer.WriteAsync(createMessage reply, cancellationToken)
                return! reply.Task
            }

    let private reserve (handle: Handle) uploadId requested =
        ask handle (fun message -> handle.Channel.Writer.TryWrite(message)) (fun reply -> Reserve(uploadId, requested, reply))

    let private release (handle: Handle) uploadId bytes =
        askRequired handle CancellationToken.None (fun reply -> Release(uploadId, bytes, reply))

    let private saveCoreAsync (handle: Handle) (fileName: string) (reader: PipeReader) (cancellationToken: CancellationToken) =
            let work = task {
                let uploadId = Guid.NewGuid()
                let chunks = ResizeArray<byte array>()
                let mutable uploadedBytes = 0L
                let mutable reservedBytes = 0L
                let mutable finished = false
                let mutable result: Result<StoredFileInfo, StoreError> option = None

                try
                    while not finished && result.IsNone do
                        cancellationToken.ThrowIfCancellationRequested()

                        if uploadedBytes = MaxFileSize then
                            let! probe = reader.ReadAsync(cancellationToken)

                            if probe.Buffer.IsEmpty && probe.IsCompleted then
                                reader.AdvanceTo(probe.Buffer.End)
                                finished <- true
                            elif probe.Buffer.IsEmpty then
                                reader.AdvanceTo(probe.Buffer.Start, probe.Buffer.End)
                            else
                                let consumed = probe.Buffer.GetPosition(1L)
                                reader.AdvanceTo(consumed)
                                result <- Some(Error TooLarge)
                        else
                            let requested = min (int64 ChunkSize) (MaxFileSize - uploadedBytes)
                            let! reservation = reserve handle uploadId requested

                            match reservation with
                            | Error error -> result <- Some(Error error)
                            | Ok granted when granted = 0L ->
                                // Capacity may be exactly exhausted by this upload. Read one
                                // byte to distinguish an exact-limit file from an oversized one.
                                let! probe = reader.ReadAsync(cancellationToken)

                                if probe.Buffer.IsEmpty && probe.IsCompleted then
                                    reader.AdvanceTo(probe.Buffer.End)
                                    finished <- true
                                elif probe.Buffer.IsEmpty then
                                    reader.AdvanceTo(probe.Buffer.Start, probe.Buffer.End)
                                else
                                    let consumed = probe.Buffer.GetPosition(1L)
                                    reader.AdvanceTo(consumed)
                                    result <- Some(Error TooLarge)
                            | Ok granted ->
                                reservedBytes <- reservedBytes + granted
                                let! readResult = reader.ReadAsync(cancellationToken)
                                let buffer = readResult.Buffer
                                let count = min granted buffer.Length

                                if count > 0L then
                                    let data = Array.zeroCreate<byte> (int count)
                                    let mutable copied = 0
                                    let sequence = buffer.Slice(0L, count)
                                    let mutable position = sequence.Start
                                    let mutable copying = true

                                    while copying do
                                        let mutable segment = ReadOnlyMemory<byte>.Empty

                                        if sequence.TryGet(&position, &segment, true) then
                                            segment.Span.CopyTo(data.AsSpan(copied))
                                            copied <- copied + segment.Length
                                        else
                                            copying <- false

                                    chunks.Add(data)
                                    uploadedBytes <- uploadedBytes + count

                                let consumed = buffer.GetPosition(count)
                                reader.AdvanceTo(consumed, consumed)

                                let unused = granted - count

                                if unused > 0L then
                                    do! release handle uploadId unused
                                    reservedBytes <- reservedBytes - unused

                                if readResult.IsCompleted && count = 0L then
                                    finished <- true
                                elif count = 0L && not readResult.IsCompleted then
                                    ()

                    match result with
                    | Some(Error error) ->
                        if reservedBytes > 0L then
                            do! release handle uploadId reservedBytes
                            reservedBytes <- 0L

                        return Error error
                    | Some(Ok info) -> return Ok info
                    | None when finished ->
                        let info =
                            { Id = Guid.NewGuid()
                              Name = fileName
                              SizeBytes = uploadedBytes
                              UploadedAtUtc = DateTimeOffset.UtcNow }

                        let stored =
                            { Info = info
                              Content =
                                { Chunks = chunks.ToArray()
                                  Length = uploadedBytes } }

                        let! committed =
                            askRequired handle cancellationToken (fun reply -> Commit(uploadId, stored, reply))

                        reservedBytes <- 0L
                        return committed
                    | None -> return Error TooLarge
                with error ->
                    if reservedBytes > 0L then
                        try
                            do! release handle uploadId reservedBytes
                        with _ -> ()

                    return raise error
            }

            task {
                try
                    return! work
                finally
                    reader.Complete()
            }

    let list (handle: Handle) =
        ask handle (fun message -> handle.Channel.Writer.TryWrite(message)) List

    let tryOpen (handle: Handle) id =
        ask handle (fun message -> handle.Channel.Writer.TryWrite(message)) (fun reply -> TryOpen(id, reply))

    let saveAsync (handle: Handle) fileName reader cancellationToken =
        saveCoreAsync handle fileName reader cancellationToken

    let deleteMany (handle: Handle) ids =
        ask handle (fun message -> handle.Channel.Writer.TryWrite(message)) (fun reply -> DeleteMany(ids, reply))
