namespace FileShare

open System
open System.IO
open System.Net
open System.Net.NetworkInformation
open System.Net.Sockets
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Server.Kestrel.Core
open Microsoft.AspNetCore.WebUtilities
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open Microsoft.Net.Http.Headers

type ServerOptions =
    { Address: IPAddress
      Port: int }

module private CommandLine =
    let usage = "Usage: FileShareServer --ip <IPv4-or-IPv6-address> --port <1-65535>"

    let parse (args: string array) =
        let rec loop remaining address port =
            match remaining with
            | [] ->
                match address, port with
                | Some parsedAddress, Some parsedPort -> Ok { Address = parsedAddress; Port = parsedPort }
                | _ -> Error $"--ip と --port は必須です。{Environment.NewLine}{usage}"
            | "--ip" :: value :: tail ->
                match address, IPAddress.TryParse(value) with
                | Some _, _ -> Error "--ip は一度だけ指定してください。"
                | None, (true, parsedAddress) -> loop tail (Some parsedAddress) port
                | None, _ -> Error $"IPアドレス '{value}' はIPv4またはIPv6の形式ではありません。"
            | "--port" :: value :: tail ->
                match port, Int32.TryParse(value) with
                | Some _, _ -> Error "--port は一度だけ指定してください。"
                | None, (true, parsedPort) when parsedPort >= 1 && parsedPort <= 65535 ->
                    loop tail address (Some parsedPort)
                | None, _ -> Error $"ポート '{value}' は1から65535の整数ではありません。"
            | "--ip" :: []
            | "--port" :: [] -> Error $"引数の値がありません。{Environment.NewLine}{usage}"
            | unknown :: _ -> Error $"不明な引数 '{unknown}' です。{Environment.NewLine}{usage}"

        loop (args |> Array.toList) None None

module private Url =
    let format (address: IPAddress) port =
        if address.AddressFamily = AddressFamily.InterNetworkV6 then
            $"http://[{address}]:{port}/"
        else
            $"http://{address}:{port}/"

    let private firstUsableAddress family =
        NetworkInterface.GetAllNetworkInterfaces()
        |> Seq.filter (fun networkInterface -> networkInterface.OperationalStatus = OperationalStatus.Up)
        |> Seq.collect (fun networkInterface -> networkInterface.GetIPProperties().UnicastAddresses)
        |> Seq.map (fun unicastAddress -> unicastAddress.Address)
        |> Seq.tryFind (fun address ->
            address.AddressFamily = family
            && not (IPAddress.IsLoopback(address))
            && not (family = AddressFamily.InterNetworkV6 && address.IsIPv6LinkLocal))

    let displayAddress configuredAddress =
        if configuredAddress.Equals(IPAddress.Any) then
            firstUsableAddress AddressFamily.InterNetwork |> Option.defaultValue IPAddress.Loopback
        elif configuredAddress.Equals(IPAddress.IPv6Any) then
            firstUsableAddress AddressFamily.InterNetworkV6 |> Option.defaultValue IPAddress.IPv6Loopback
        else
            configuredAddress

module private Multipart =
    let tryGetBoundary (contentType: string) =
        if String.IsNullOrWhiteSpace contentType then
            None
        else
            try
                let mediaType = MediaTypeHeaderValue.Parse(contentType)

                if not (String.Equals(mediaType.MediaType.Value, "multipart/form-data", StringComparison.OrdinalIgnoreCase)) then
                    None
                else
                    let boundary = HeaderUtilities.RemoveQuotes(mediaType.Boundary).Value

                    if String.IsNullOrWhiteSpace boundary || boundary.Length > 200 then
                        None
                    else
                        Some boundary
            with :? FormatException ->
                None

    let tryGetFileName (section: MultipartSection) =
        if String.IsNullOrWhiteSpace section.ContentDisposition then
            None
        else
            try
                let disposition = ContentDispositionHeaderValue.Parse(section.ContentDisposition)

                if not (String.Equals(disposition.DispositionType.Value, "form-data", StringComparison.OrdinalIgnoreCase)) then
                    None
                else
                    let starName = HeaderUtilities.RemoveQuotes(disposition.FileNameStar).Value
                    let normalName = HeaderUtilities.RemoveQuotes(disposition.FileName).Value

                    if not (String.IsNullOrWhiteSpace starName) then Some starName
                    elif not (String.IsNullOrWhiteSpace normalName) then Some normalName
                    else None
            with :? FormatException ->
                None

module private EmbeddedWebRoot =
    let private assembly = typeof<InMemoryFileStore>.Assembly
    let private resourcePrefix = assembly.GetName().Name + ".wwwroot."

    let private contentType (path: string) =
        match Path.GetExtension(path).ToLowerInvariant() with
        | ".html" -> "text/html; charset=utf-8"
        | ".css" -> "text/css; charset=utf-8"
        | ".js" -> "text/javascript; charset=utf-8"
        | ".json" -> "application/json; charset=utf-8"
        | _ -> "application/octet-stream"

    let tryOpen (requestPath: string) =
        let normalizedPath =
            if String.IsNullOrWhiteSpace requestPath then ""
            else requestPath.Trim('/')

        let candidates =
            if String.IsNullOrEmpty normalizedPath then
                [ "index.html" ]
            elif Path.HasExtension(normalizedPath) then
                [ normalizedPath ]
            else
                [ normalizedPath + "/index.html" ]

        candidates
        |> List.tryPick (fun resourcePath ->
            let resourceName = resourcePrefix + resourcePath.Replace('/', '.')
            let stream = assembly.GetManifestResourceStream(resourceName)

            if isNull stream then None
            else Some(stream, contentType resourcePath))

module Program =
    let private serveEmbeddedResource requestPath : IResult =
        match EmbeddedWebRoot.tryOpen requestPath with
        | Some(stream, contentType) -> Results.Stream(stream, contentType)
        | None -> Results.NotFound()

    let private requestJsonOptions =
        let options = JsonSerializerOptions()
        options.PropertyNameCaseInsensitive <- true
        options

    let private uploadFiles (request: HttpRequest) (store: IFileStore) (cancellationToken: CancellationToken) =
        task {
            match Multipart.tryGetBoundary request.ContentType with
            | None ->
                return Results.BadRequest("multipart/form-data 形式でファイルを送信してください。")
            | Some boundary ->
                let reader = MultipartReader(boundary, request.Body)
                let uploaded = ResizeArray<StoredFileInfo>()
                let mutable reading = true
                let mutable validationError: string option = None

                while reading do
                    let! section = reader.ReadNextSectionAsync(cancellationToken)

                    if isNull section then
                        reading <- false
                    else
                        match Multipart.tryGetFileName section with
                        | Some uploadedName ->
                            match FileName.tryNormalize uploadedName with
                            | Ok normalizedName ->
                                let! stored = store.SaveAsync(normalizedName, section.Body, cancellationToken)
                                uploaded.Add(stored)
                            | Error message ->
                                validationError <- Some message
                                do! section.Body.CopyToAsync(Stream.Null, cancellationToken)
                        | None ->
                            do! section.Body.CopyToAsync(Stream.Null, cancellationToken)

                match validationError with
                | Some message -> return Results.BadRequest(message)
                | None when uploaded.Count = 0 ->
                    return Results.BadRequest("アップロードするファイルを指定してください。")
                | None -> return Results.Ok(uploaded.ToArray())
        }

    let private deleteFiles (request: HttpRequest) (store: IFileStore) (cancellationToken: CancellationToken) =
        task {
            try
                let! payload =
                    JsonSerializer.DeserializeAsync<DeleteRequest>(request.Body, requestJsonOptions, cancellationToken).AsTask()

                if isNull (box payload) || isNull payload.Ids then
                    return Results.BadRequest("ids 配列を指定してください。")
                else
                    let deleted = store.DeleteMany(payload.Ids)
                    return Results.Ok({ Deleted = deleted })
            with :? JsonException ->
                return Results.BadRequest("削除対象のJSON形式が正しくありません。")
        }

    [<EntryPoint>]
    let main args =
        match CommandLine.parse args with
        | Error message ->
            Console.Error.WriteLine(message)
            2
        | Ok settings ->
            let builder = WebApplication.CreateBuilder()
            builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Warning) |> ignore

            builder.WebHost.ConfigureKestrel(fun options ->
                // The logical 4 GiB limit is enforced by InMemoryFileStore while streaming.
                // Kestrel itself must not reject a multipart body merely because it has headers.
                options.Limits.MaxRequestBodySize <- Nullable<int64>()
                options.Listen(settings.Address, settings.Port))
            |> ignore

            builder.Services.AddSingleton<IFileStore>(InMemoryFileStore() :> IFileStore) |> ignore

            let app = builder.Build()
            app.MapGet("/swagger", Func<IResult>(fun () -> Results.Redirect("/docs/"))) |> ignore

            app.MapGet(
                "/api/files",
                Func<IFileStore, IResult>(fun store -> Results.Ok(store.List()))
            )
            |> ignore

            app.MapPost(
                "/api/files",
                Func<HttpRequest, IFileStore, CancellationToken, Task<IResult>>(uploadFiles)
            )
            |> ignore

            app.MapDelete(
                "/api/files",
                Func<HttpRequest, IFileStore, CancellationToken, Task<IResult>>(deleteFiles)
            )
            |> ignore

            app.MapGet(
                "/api/files/{id:guid}/download",
                Func<Guid, IFileStore, IResult>(fun id store ->
                    match store.TryOpen(id) with
                    | Some file -> Results.File(file.OpenRead(), "application/octet-stream", file.Name)
                    | None -> Results.NotFound())
            )
            |> ignore

            app.MapGet("/", Func<IResult>(fun () -> serveEmbeddedResource "")) |> ignore
            app.MapGet(
                "/{**path}",
                Func<HttpRequest, IResult>(fun request -> serveEmbeddedResource request.Path.Value)
            )
            |> ignore

            let indexUrl = Url.format (Url.displayAddress settings.Address) settings.Port

            app.Lifetime.ApplicationStarted.Register(fun () ->
                Console.WriteLine($"Index URL: {indexUrl}"))
            |> ignore

            app.Run()
            0
