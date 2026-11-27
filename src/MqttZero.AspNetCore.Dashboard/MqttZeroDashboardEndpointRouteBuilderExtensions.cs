using MqttZero.Server.Broker;
using MqttZero.Protocol;
using System.Buffers;
using System.Text;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 映射管理后台端点。
/// </summary>
public static class MqttZeroDashboardEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps the dashboard UI and API endpoints under a route prefix.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="prefix">The route prefix, for example <c>/mqttzero</c>.</param>
    /// <returns>The endpoint route builder.</returns>
    /// <exception cref="InvalidOperationException">Thrown when authorization policies are not configured.</exception>
    public static IEndpointRouteBuilder MapMqttZeroDashboard(
        this IEndpointRouteBuilder endpoints,
        string prefix = "/mqttzero")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);

        MqttZeroDashboardOptions options = endpoints.ServiceProvider
            .GetRequiredService<IOptions<MqttZeroDashboardOptions>>()
            .Value;
        ValidateOptions(options);

        string normalizedPrefix = "/" + prefix.Trim('/');
        RouteGroupBuilder group = endpoints.MapGroup(normalizedPrefix)
            .RequireAuthorization(options.ReadAuthorizationPolicy);
        group.AddEndpointFilter(AddSecurityHeadersAsync);

        group.MapGet("/api/v1/overview", GetOverview);
        group.MapGet("/api/v1/clients", GetClients);
        group.MapGet("/api/v1/sessions", GetSessions);
        group.MapGet("/api/v1/antiforgery", GetAntiforgeryToken);
        group.MapPost("/api/v1/publish", PublishAsync)
            .RequireAuthorization(options.OperateAuthorizationPolicy);

        group.MapGet("/", GetIndex);
        group.MapGet("/assets/{**path}", GetAsset);
        endpoints.MapFallback($"{normalizedPrefix}/{{**path}}", GetIndex)
            .RequireAuthorization(options.ReadAuthorizationPolicy)
            .AddEndpointFilter(AddSecurityHeadersAsync);
        return endpoints;
    }

    /// <summary>
    /// 校验选项。
    /// </summary>
    /// <param name="options">选项。</param>
    private static void ValidateOptions(MqttZeroDashboardOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ReadAuthorizationPolicy) ||
            string.IsNullOrWhiteSpace(options.OperateAuthorizationPolicy))
        {
            throw new InvalidOperationException(
                "MQTTZero dashboard authorization policies must be configured before mapping the dashboard.");
        }

        if (options.DefaultPageSize <= 0 ||
            options.MaxPageSize < options.DefaultPageSize)
        {
            throw new InvalidOperationException("MQTTZero dashboard option values are invalid.");
        }
    }

    /// <summary>
    /// 获取Overview。
    /// </summary>
    /// <param name="broker">对应的 broker。</param>
    /// <returns>对应的结果。</returns>
    private static IResult GetOverview(IMqttZeroBroker broker)
    {
        return Results.Json(
            new DashboardOverviewDto(
                broker.State.ToString(),
                broker.ActiveConnectionCount,
                broker.ConnectedClientCount),
            DashboardJsonSerializerContext.Default.DashboardOverviewDto);
    }

    /// <summary>
    /// 获取客户端。
    /// </summary>
    /// <param name="broker">对应的 broker。</param>
    /// <param name="configuredOptions">configured选项。</param>
    /// <param name="page">对应的 page。</param>
    /// <param name="pageSize">page大小。</param>
    /// <returns>对应的结果。</returns>
    private static IResult GetClients(
        IMqttZeroBroker broker,
        IOptions<MqttZeroDashboardOptions> configuredOptions,
        int page = 1,
        int pageSize = 0)
    {
        DashboardClientDto[] allClients = broker.GetClients()
            .OrderBy(static client => client.ClientId, StringComparer.Ordinal)
            .Select(static client => new DashboardClientDto(
                client.ClientId,
                client.RemoteEndPoint?.ToString(),
                client.IsSecure,
                client.IsWebSocket,
                client.SubscriptionCount))
            .ToArray();
        DashboardClientPageDto result = CreatePage(
            allClients,
            configuredOptions.Value,
            page,
            pageSize);
        return Results.Json(
            result,
            DashboardJsonSerializerContext.Default.DashboardClientPageDto);
    }

    /// <summary>
    /// 获取Sessions。
    /// </summary>
    /// <param name="broker">对应的 broker。</param>
    /// <param name="configuredOptions">configured选项。</param>
    /// <param name="page">对应的 page。</param>
    /// <param name="pageSize">page大小。</param>
    /// <returns>对应的结果。</returns>
    private static IResult GetSessions(
        IMqttZeroBroker broker,
        IOptions<MqttZeroDashboardOptions> configuredOptions,
        int page = 1,
        int pageSize = 0)
    {
        DashboardSessionDto[] allSessions = broker.GetSessions()
            .OrderBy(static session => session.ClientId, StringComparer.Ordinal)
            .Select(static session => new DashboardSessionDto(
                session.ClientId,
                session.IsConnected,
                session.CleanStart,
                session.SessionPresent,
                session.ExpiryIntervalSeconds,
                session.ExpiresAt,
                session.SubscriptionCount,
                session.PendingMessageCount,
                session.InflightMessageCount,
                session.HasWill,
                session.WillTopic))
            .ToArray();
        DashboardSessionPageDto result = CreatePage(
            allSessions,
            configuredOptions.Value,
            page,
            pageSize);
        return Results.Json(
            result,
            DashboardJsonSerializerContext.Default.DashboardSessionPageDto);
    }

    /// <summary>
    /// 创建Page。
    /// </summary>
    /// <param name="items">项。</param>
    /// <param name="options">选项。</param>
    /// <param name="page">对应的 page。</param>
    /// <param name="pageSize">page大小。</param>
    /// <returns>对应的结果。</returns>
    private static DashboardClientPageDto CreatePage(
        DashboardClientDto[] items,
        MqttZeroDashboardOptions options,
        int page,
        int pageSize)
    {
        PageSelection selection = SelectPage(items.Length, options, page, pageSize);
        return new DashboardClientPageDto(
            items.Skip(selection.Offset).Take(selection.Size).ToArray(),
            selection.Page,
            selection.Size,
            items.Length);
    }

    /// <summary>
    /// 创建Page。
    /// </summary>
    /// <param name="items">项。</param>
    /// <param name="options">选项。</param>
    /// <param name="page">对应的 page。</param>
    /// <param name="pageSize">page大小。</param>
    /// <returns>对应的结果。</returns>
    private static DashboardSessionPageDto CreatePage(
        DashboardSessionDto[] items,
        MqttZeroDashboardOptions options,
        int page,
        int pageSize)
    {
        PageSelection selection = SelectPage(items.Length, options, page, pageSize);
        return new DashboardSessionPageDto(
            items.Skip(selection.Offset).Take(selection.Size).ToArray(),
            selection.Page,
            selection.Size,
            items.Length);
    }

    /// <summary>
    /// 选择Page。
    /// </summary>
    /// <param name="total">对应的 total。</param>
    /// <param name="options">选项。</param>
    /// <param name="page">对应的 page。</param>
    /// <param name="pageSize">page大小。</param>
    /// <returns>对应的结果。</returns>
    private static PageSelection SelectPage(
        int total,
        MqttZeroDashboardOptions options,
        int page,
        int pageSize)
    {
        int size = pageSize <= 0
            ? options.DefaultPageSize
            : Math.Clamp(pageSize, 1, options.MaxPageSize);
        int maximumPage = Math.Max(1, (total + size - 1) / size);
        int selectedPage = Math.Clamp(page, 1, maximumPage);
        return new PageSelection(
            selectedPage,
            size,
            (selectedPage - 1) * size);
    }

    /// <summary>
    /// 表示 struct 的类型。
    /// </summary>
    private readonly record struct PageSelection(
        int Page,
        int Size,
        int Offset);

    /// <summary>
    /// 获取Antiforgery令牌。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="antiforgery">对应的 antiforgery。</param>
    /// <returns>对应的结果。</returns>
    private static IResult GetAntiforgeryToken(HttpContext context, IAntiforgery antiforgery)
    {
        AntiforgeryTokenSet tokens = antiforgery.GetAndStoreTokens(context);
        return Results.Json(new { token = tokens.RequestToken });
    }

    /// <summary>
    /// 发布。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="antiforgery">对应的 antiforgery。</param>
    /// <param name="broker">对应的 broker。</param>
    /// <param name="request">对应的 request。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    private static async Task<IResult> PublishAsync(
        HttpContext context,
        IAntiforgery antiforgery,
        IMqttZeroBroker broker,
        DashboardPublishRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            await antiforgery.ValidateRequestAsync(context).ConfigureAwait(false);
        }
        catch (AntiforgeryValidationException)
        {
            return Results.BadRequest();
        }

        await broker.PublishAsync(
            request.Topic,
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(request.Payload)),
            request.QualityOfServiceLevel,
            request.Retain,
            cancellationToken).ConfigureAwait(false);
        return Results.NoContent();
    }

    /// <summary>
    /// 获取索引。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>对应的结果。</returns>
    private static IResult GetIndex(HttpContext context)
    {
        context.Response.Headers.CacheControl = "no-store";
        return GetAssetResult("index.html");
    }

    /// <summary>
    /// 获取Asset。
    /// </summary>
    /// <param name="path">路径。</param>
    /// <param name="context">上下文。</param>
    /// <returns>对应的结果。</returns>
    private static IResult GetAsset(string path, HttpContext context)
    {
        context.Response.Headers.CacheControl = "public,max-age=31536000,immutable";
        return GetAssetResult($"assets/{path.TrimStart('/')}");
    }

    /// <summary>
    /// 获取Asset结果。
    /// </summary>
    /// <param name="path">路径。</param>
    /// <returns>对应的结果。</returns>
    private static IResult GetAssetResult(string path)
    {
        if (!MqttZeroDashboardAssets.TryOpen(path, out Stream? stream, out string contentType) ||
            stream == null)
        {
            return Results.NotFound();
        }

        return Results.Stream(stream, contentType);
    }

    /// <summary>
    /// 添加SecurityHeaders。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="next">下一个。</param>
    /// <returns>异步操作的结果。</returns>
    private static async ValueTask<object?> AddSecurityHeadersAsync(
        EndpointFilterInvocationContext context,
        EndpointFilterDelegate next)
    {
        context.HttpContext.Response.Headers.XContentTypeOptions = "nosniff";
        context.HttpContext.Response.Headers.XFrameOptions = "DENY";
        context.HttpContext.Response.Headers.ContentSecurityPolicy =
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
            "img-src 'self' data:; connect-src 'self' ws: wss:; frame-ancestors 'none'";
        context.HttpContext.Response.Headers["Referrer-Policy"] = "no-referrer";
        return await next(context).ConfigureAwait(false);
    }
}
