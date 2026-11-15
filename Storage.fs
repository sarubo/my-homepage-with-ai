namespace FileShare

open System
open System.Collections.Generic
open System.IO
open System.Threading
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

type FileDownload =
    { Name: string
      SizeBytes: int64
      OpenRead: unit -> Stream }

type IFileStore =
    abstract List: unit -> StoredFileInfo list
    abstract TryOpen: id: Guid -> FileDownload option
    abstract SaveAsync: fileName: string * content: Stream * cancellationToken: CancellationToken -> Task<StoredFileInfo>
    abstract DeleteMany: ids: Guid array -> int

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

type private ChunkedContent =
    { Chunks: byte array array
      Length: int64 }

type private StoredFile =
    { Info: StoredFileInfo
      Content: ChunkedContent }

/// Read-only stream over fixed-size memory chunks.  A chunk is intentionally much
/// smaller than the CLR array-size limit, so a single 4 GiB upload is supported.
type private ChunkedReadStream(chunks: byte array array, length: int64, chunkSize: int) =
    inherit Stream()

    let mutable position = 0L

    member private _.ReadCore(destination: Span<byte>) =
        if position >= length || destination.Length = 0 then
            0
        else
            let available = min (int64 destination.Length) (length - position)
            let mutable copied = 0
            let mutable remaining = int available

            while remaining > 0 do
                let chunkIndex = int (position / int64 chunkSize)
                let chunkOffset = int (position % int64 chunkSize)
                let fromChunk = min remaining (chunks.[chunkIndex].Length - chunkOffset)

                chunks.[chunkIndex].AsSpan(chunkOffset, fromChunk).CopyTo(destination.Slice(copied, fromChunk))

                position <- position + int64 fromChunk
                copied <- copied + fromChunk
                remaining <- remaining - fromChunk

            copied

    override _.CanRead = true
    override _.CanSeek = true
    override _.CanWrite = false
    override _.Length = length

    override _.Position
        with get () = position
        and set value =
            if value < 0L || value > length then
                raise (ArgumentOutOfRangeException(nameof value))

            position <- value

    override _.Flush() = ()

    override this.Read(buffer: byte array, offset: int, count: int) =
        if isNull buffer then
            nullArg (nameof buffer)

        if offset < 0 || count < 0 || offset > buffer.Length - count then
            raise (ArgumentOutOfRangeException())

        this.ReadCore(buffer.AsSpan(offset, count))

    override this.ReadAsync(buffer: Memory<byte>, cancellationToken: CancellationToken) =
        cancellationToken.ThrowIfCancellationRequested()
        ValueTask<int>(this.ReadCore(buffer.Span))

    override _.Seek(offset: int64, origin: SeekOrigin) =
        let newPosition =
            match origin with
            | SeekOrigin.Begin -> offset
            | SeekOrigin.Current -> position + offset
            | SeekOrigin.End -> length + offset
            | _ -> raise (ArgumentOutOfRangeException(nameof origin))

        if newPosition < 0L || newPosition > length then
            raise (IOException("指定された位置はファイル範囲外です。"))

        position <- newPosition
        position

    override _.SetLength(_: int64) = raise (NotSupportedException())
    override _.Write(_: byte array, _: int, _: int) = raise (NotSupportedException())

/// A restart-scoped file store.  Content is chunked because .NET arrays cannot
/// represent a 4 GiB file as one value.  The IFileStore boundary permits a future
/// file-system or database implementation without changing HTTP handlers.
type InMemoryFileStore() =
    static let ChunkSize = 4 * 1024 * 1024
    static let MaxFileSize = 4L * 1024L * 1024L * 1024L
    static let MaxTotalSize = 4L * 1024L * 1024L * 1024L

    let syncRoot = obj ()
    let files = Dictionary<string, StoredFile>(StringComparer.Ordinal)
    let mutable usedBytes = 0L

    member private _.FailForCapacity(message: string) =
        Console.Error.WriteLine($"FATAL: {message}")
        Environment.FailFast(message)

    member private _.FailForOutOfMemory(error: OutOfMemoryException) =
        let message = "RAM の確保に失敗したため、ファイル共有サーバーを停止します。"
        Console.Error.WriteLine($"FATAL: {message}{Environment.NewLine}{error}")
        Environment.FailFast(message, error)

    member private this.Reserve(bytes: int64) =
        lock syncRoot (fun () ->
            if bytes < 0L || usedBytes > MaxTotalSize - bytes then
                this.FailForCapacity(
                    $"RAM ストアの合計上限 ({MaxTotalSize} bytes) を超えるアップロードを受け取りました。"
                )

                raise (InvalidOperationException("Environment.FailFast returned unexpectedly."))

            usedBytes <- usedBytes + bytes)

    member private _.Release(bytes: int64) =
        if bytes > 0L then
            lock syncRoot (fun () -> usedBytes <- max 0L (usedBytes - bytes))

    member private _.ListFiles() =
        lock syncRoot (fun () ->
            files.Values
            |> Seq.map (fun file -> file.Info)
            |> Seq.sortBy (fun file -> file.Name)
            |> Seq.toList)

    member private _.TryOpenFile(id: Guid) =
        lock syncRoot (fun () ->
            files.Values
            |> Seq.tryFind (fun file -> file.Info.Id = id)
            |> Option.map (fun file ->
                { Name = file.Info.Name
                  SizeBytes = file.Info.SizeBytes
                  OpenRead = fun () -> new ChunkedReadStream(file.Content.Chunks, file.Content.Length, ChunkSize) :> Stream }))

    member private this.SaveFileAsync(fileName: string, content: Stream, cancellationToken: CancellationToken) =
        task {
            let chunks = ResizeArray<byte array>()
            let mutable currentChunk: byte array = null
            let mutable currentLength = 0
            let mutable uploadedBytes = 0L
            let mutable reservedBytes = 0L
            let mutable committed = false
            let mutable reading = true

            try
                while reading do
                    if isNull currentChunk then
                        currentChunk <- Array.zeroCreate<byte> ChunkSize
                        currentLength <- 0

                    let! read =
                        content
                            .ReadAsync(currentChunk.AsMemory(currentLength, ChunkSize - currentLength), cancellationToken)
                            .AsTask()

                    if read = 0 then
                        reading <- false
                    else
                        let read64 = int64 read

                        if uploadedBytes > MaxFileSize - read64 then
                            this.FailForCapacity(
                                $"ファイル '{fileName}' が1ファイル当たりの上限 ({MaxFileSize} bytes) を超えました。"
                            )

                            raise (InvalidOperationException("Environment.FailFast returned unexpectedly."))

                        this.Reserve(read64)
                        reservedBytes <- reservedBytes + read64
                        uploadedBytes <- uploadedBytes + read64
                        currentLength <- currentLength + read

                        if currentLength = ChunkSize then
                            chunks.Add(currentChunk)
                            currentChunk <- null
                            currentLength <- 0

                if currentLength > 0 then
                    let finalChunk = Array.zeroCreate<byte> currentLength
                    Buffer.BlockCopy(currentChunk, 0, finalChunk, 0, currentLength)
                    chunks.Add(finalChunk)

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

                lock syncRoot (fun () ->
                    let found, previous = files.TryGetValue(fileName)
                    files.[fileName] <- stored

                    if found then
                        usedBytes <- usedBytes - previous.Content.Length)

                committed <- true
                return info
            with
            | :? OutOfMemoryException as error ->
                if not committed then
                    this.Release(reservedBytes)

                this.FailForOutOfMemory(error)
                return raise (InvalidOperationException("Environment.FailFast returned unexpectedly."))
            | error ->
                if not committed then
                    this.Release(reservedBytes)

                return raise error
        }

    member private _.DeleteFiles(ids: Guid array) =
        if isNull ids then
            0
        else
            lock syncRoot (fun () ->
                let selected = HashSet<Guid>(ids)

                let namesToDelete =
                    files
                    |> Seq.choose (fun pair -> if selected.Contains(pair.Value.Info.Id) then Some pair.Key else None)
                    |> Seq.toArray

                for name in namesToDelete do
                    let file = files.[name]
                    files.Remove(name) |> ignore
                    usedBytes <- usedBytes - file.Content.Length

                namesToDelete.Length)

    interface IFileStore with
        member this.List() = this.ListFiles()
        member this.TryOpen(id) = this.TryOpenFile(id)
        member this.SaveAsync(fileName, content, cancellationToken) = this.SaveFileAsync(fileName, content, cancellationToken)
        member this.DeleteMany(ids) = this.DeleteFiles(ids)
