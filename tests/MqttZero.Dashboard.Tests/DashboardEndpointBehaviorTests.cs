using System.Buffers;
using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MqttZero.AspNetCore;
using MqttZero.AspNetCore.Dashboard;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Retained;
using MqttZero.Server.Sessions;

namespace MqttZero.Dashboard.Tests
{
    public sealed class DashboardEndpointBehaviorTests
    {
        [Fact(Timeout = 15_000)]
        public async Task ListsUseStableOrderingAndClampPagination()
        {
            TestContext.Current.CancellationToken.ThrowIfCancellationRequested();
            FakeBroker broker = new()
            {
                Clients =
                [
                    CreateClient("b"),
                    CreateClient("a"),
                    CreateClient("c")
                ],
                Sessions =
                [
                    CreateSession("session-z"),
                    CreateSession("session-a")
                ]
            };
            await using WebApplication app = await StartAppAsync(
                broker,
                options =>
                {
                    options.DefaultPageSize = 2;
                    options.MaxPageSize = 2;
                });
            using HttpClient client = CreateClient(app);

            using JsonDocument clients = await GetJsonAsync(
                client,
                "/admin/api/v1/clients?page=99&pageSize=1");
            using JsonDocument sessions = await GetJsonAsync(
                client,
                "/admin/api/v1/sessions?page=-5&pageSize=99");

            JsonElement clientPage = clients.RootElement;
            Assert.Equal(3, clientPage.GetProperty("total").GetInt32());
            Assert.Equal(3, clientPage.GetProperty("page").GetInt32());
            Assert.Equal("c", clientPage.GetProperty("items")[0]
                .GetProperty("clientId").GetString());
            JsonElement sessionPage = sessions.RootElement;
            Assert.Equal(2, sessionPage.GetProperty("pageSize").GetInt32());
            Assert.Equal(1, sessionPage.GetProperty("page").GetInt32());
            Assert.Equal("session-a", sessionPage.GetProperty("items")[0]
                .GetProperty("clientId").GetString());
            Assert.Equal("session-z", sessionPage.GetProperty("items")[1]
                .GetProperty("clientId").GetString());
        }

        [Fact(Timeout = 15_000)]
        public async Task DashboardAddsSecurityHeadersAndAssetNotFound()
        {
            await using WebApplication app = await StartAppAsync(new FakeBroker());
            using HttpClient client = CreateClient(app);
            using HttpResponseMessage index = await client.GetAsync(
                "/admin/",
                TestContext.Current.CancellationToken);
            using HttpResponseMessage missingAsset = await client.GetAsync(
                "/admin/assets/not-found.js",
                TestContext.Current.CancellationToken);
            string indexHtml = await index.Content.ReadAsStringAsync(
                TestContext.Current.CancellationToken);
            string scriptPath = ExtractAssetPath(indexHtml, ".js");
            using HttpResponseMessage script = await client.GetAsync(
                $"/admin/{scriptPath}",
                TestContext.Current.CancellationToken);

            Assert.Equal(HttpStatusCode.OK, index.StatusCode);
            Assert.Equal("nosniff", index.Headers.GetValues("X-Content-Type-Options").Single());
            Assert.Equal("DENY", index.Headers.GetValues("X-Frame-Options").Single());
            Assert.Equal("no-referrer", index.Headers.GetValues("Referrer-Policy").Single());
            Assert.Equal(HttpStatusCode.NotFound, missingAsset.StatusCode);
            Assert.Equal(HttpStatusCode.OK, script.StatusCode);
            Assert.Equal(
                "text/javascript",
                script.Content.Headers.ContentType!.MediaType);
        }

        [Fact(Timeout = 15_000)]
        public async Task PublishWithoutAntiforgeryIsRejected()
        {
            await using WebApplication app = await StartAppAsync(new FakeBroker());
            using HttpClient client = CreateClient(app);

            using HttpResponseMessage response = await client.PostAsJsonAsync(
                "/admin/api/v1/publish",
                new
                {
                    topic = "dashboard/test",
                    payload = "value",
                    qualityOfServiceLevel = 0,
                    retain = false
                },
                TestContext.Current.CancellationToken);

            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Fact(Timeout = 15_000)]
        public async Task PublishingMapsRequestToBrokerMessage()
        {
            FakeBroker broker = new();
            await using WebApplication app = await StartAppAsync(broker);
            using HttpClient client = CreateClient(app);
            using JsonDocument token = await GetJsonAsync(
                client,
                "/admin/api/v1/antiforgery");
            using HttpRequestMessage request = new(
                HttpMethod.Post,
                "/admin/api/v1/publish")
            {
                Content = JsonContent.Create(new
                {
                    topic = "dashboard/retained",
                    payload = "payload",
                    qualityOfServiceLevel = 1,
                    retain = true
                })
            };
            request.Headers.Add(
                "RequestVerificationToken",
                token.RootElement.GetProperty("token").GetString());

            using HttpResponseMessage response = await client.SendAsync(
                request,
                TestContext.Current.CancellationToken);

            Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
            Assert.NotNull(broker.Published);
            Assert.Equal("dashboard/retained", broker.Published.Topic);
            Assert.Equal(
                "payload",
                System.Text.Encoding.UTF8.GetString(
                    broker.Published.Payload.ToArray()));
            Assert.Equal(
                MqttQualityOfServiceLevel.AtLeastOnce,
                broker.Published.QualityOfServiceLevel);
            Assert.True(broker.Published.Retain);
        }

        [Fact]
        public void MappingRequiresAuthorizationPolicies()
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IMqttZeroBroker>(new FakeBroker());
            builder.Services.AddMqttZero();
            builder.Services.AddMqttZeroDashboard();
            using WebApplication app = builder.Build();

            Assert.Throws<InvalidOperationException>(
                () => app.MapMqttZeroDashboard("/admin"));
        }

        [Theory]
        [InlineData(0, 200)]
        [InlineData(10, 5)]
        public void MappingRejectsInvalidPageOptions(int defaultPageSize, int maxPageSize)
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IMqttZeroBroker>(new FakeBroker());
            builder.Services.AddMqttZero();
            builder.Services.AddMqttZeroDashboard(options =>
            {
                options.ReadAuthorizationPolicy = "Read";
                options.OperateAuthorizationPolicy = "Operate";
                options.DefaultPageSize = defaultPageSize;
                options.MaxPageSize = maxPageSize;
            });
            using WebApplication app = builder.Build();

            Assert.Throws<InvalidOperationException>(
                () => app.MapMqttZeroDashboard("/admin"));
        }

        private static async Task<WebApplication> StartAppAsync(
            FakeBroker broker,
            Action<MqttZeroDashboardOptions>? configure = null)
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
            builder.Services.AddSingleton<IMqttZeroBroker>(broker);
            builder.Services.AddMqttZero();
            builder.Services.AddMqttZeroDashboard(options =>
            {
                options.ReadAuthorizationPolicy = "Read";
                options.OperateAuthorizationPolicy = "Operate";
                configure?.Invoke(options);
            });
            builder.Services.AddAuthorizationBuilder()
                .AddPolicy("Read", static policy => policy.RequireAssertion(static _ => true))
                .AddPolicy("Operate", static policy => policy.RequireAssertion(static _ => true));
            WebApplication app = builder.Build();
            app.MapMqttZeroDashboard("/admin");
            await app.StartAsync(TestContext.Current.CancellationToken);
            return app;
        }

        private static HttpClient CreateClient(WebApplication app)
        {
            string address = app.Urls.Single();
            return new HttpClient
            {
                BaseAddress = new Uri(address)
            };
        }

        private static async Task<JsonDocument> GetJsonAsync(
            HttpClient client,
            string path)
        {
            using HttpResponseMessage response = await client.GetAsync(
                path,
                TestContext.Current.CancellationToken);
            response.EnsureSuccessStatusCode();
            Stream stream = await response.Content.ReadAsStreamAsync(
                TestContext.Current.CancellationToken);
            return await JsonDocument.ParseAsync(
                stream,
                cancellationToken: TestContext.Current.CancellationToken);
        }

        private static int GetFreePort()
        {
            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        private static string ExtractAssetPath(string html, string extension)
        {
            foreach (string value in html.Split('"'))
            {
                if ((value.StartsWith("assets/", StringComparison.Ordinal) ||
                        value.StartsWith("./assets/", StringComparison.Ordinal)) &&
                    value.EndsWith(extension, StringComparison.Ordinal))
                {
                    return value.TrimStart('.', '/');
                }
            }

            throw new InvalidOperationException(
                $"No {extension} asset was found in the dashboard index.");
        }

        private static MqttZeroClientSnapshot CreateClient(string clientId)
        {
            return new MqttZeroClientSnapshot
            {
                ClientId = clientId,
                Statistics = new MqttZeroClientStatistics()
            };
        }

        private static MqttZeroSessionSnapshot CreateSession(string clientId)
        {
            return new MqttZeroSessionSnapshot
            {
                ClientId = clientId,
                SessionPresent = true
            };
        }

        private sealed class FakeBroker : IMqttZeroBroker
        {
            public MqttZeroBrokerState State { get; set; } = MqttZeroBrokerState.Running;

            public int ActiveConnectionCount { get; set; }

            public int ConnectedClientCount { get; set; }

            public long PublishedWillCount { get; set; }

            public int MaximumPacketSize { get; set; } = 1024 * 1024;

            public IReadOnlyList<MqttZeroClientSnapshot> Clients { get; set; } = [];

            public IReadOnlyList<MqttZeroSessionSnapshot> Sessions { get; set; } = [];

            public MqttZeroPublishMessage? Published { get; private set; }

            public ValueTask PublishAsync(
                MqttZeroPublishMessage message,
                CancellationToken cancellationToken = default)
            {
                Published = message;
                return ValueTask.CompletedTask;
            }

            public IReadOnlyList<MqttZeroClientSnapshot> GetClientSnapshots()
            {
                return Clients;
            }

            public IReadOnlyList<MqttZeroSessionSnapshot> GetSessionSnapshots()
            {
                return Sessions;
            }

            public IReadOnlyList<MqttZeroRetainedMessageSnapshot> GetRetainedMessageSnapshots()
            {
                return [];
            }

            public bool TryGetClient(
                string clientId,
                out MqttZeroConnectedClient? client)
            {
                client = null;
                return false;
            }

            public ValueTask<bool> DisconnectClientAsync(
                string clientId,
                MqttReasonCode reasonCode,
                CancellationToken cancellationToken = default)
            {
                return ValueTask.FromResult(false);
            }
        }
    }
}
