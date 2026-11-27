$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$packageDirectory = Join-Path $repositoryRoot 'artifacts/packages'
$packageVersion = '0.12.0-alpha.1'
New-Item -ItemType Directory -Force -Path $packageDirectory | Out-Null
Get-ChildItem -LiteralPath $packageDirectory -Filter '*.nupkg' -File |
    Remove-Item -Force
Get-ChildItem -LiteralPath $packageDirectory -Filter '*.snupkg' -File |
    Remove-Item -Force

Push-Location $repositoryRoot
try {
    dotnet pack src/MqttZero/MqttZero.csproj -c Release --no-restore `
        -o $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'MqttZero pack failed.'
    }

    dotnet pack src/MqttZero.Server/MqttZero.Server.csproj -c Release --no-restore `
        -o $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'MqttZero.Server pack failed.'
    }

    dotnet pack src/MqttZero.AspNetCore/MqttZero.AspNetCore.csproj -c Release --no-restore `
        -o $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'MqttZero.AspNetCore pack failed.'
    }

    dotnet pack src/MqttZero.AspNetCore.Dashboard/MqttZero.AspNetCore.Dashboard.csproj `
        -c Release --no-restore -o $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'MqttZero.AspNetCore.Dashboard pack failed.'
    }

    $consumerRoot = Join-Path ([IO.Path]::GetTempPath()) (
        'mqttzero-package-consumer-' + [Guid]::NewGuid().ToString('N'))
    $sharedConsumerDirectory = Join-Path $consumerRoot 'Shared'
    New-Item -ItemType Directory -Force -Path $sharedConsumerDirectory | Out-Null
    $sharedProjectPath = Join-Path $sharedConsumerDirectory 'PackageConsumer.csproj'
    $sharedProgramPath = Join-Path $sharedConsumerDirectory 'Program.cs'

    @"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="MqttZero" Version="$packageVersion" />
  </ItemGroup>
</Project>
"@ | Set-Content -LiteralPath $sharedProjectPath -Encoding utf8NoBOM

    @'
using MqttZero.Protocol;
using MqttZero.Publishing;
using System.Buffers;

MqttZeroPublishMessage message = new()
{
    Topic = "package/common",
    Payload = new ReadOnlySequence<byte>("ok"u8.ToArray()),
    QualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce
};
_ = message.Topic;
_ = MqttReasonCode.Success;
'@ | Set-Content -LiteralPath $sharedProgramPath -Encoding utf8NoBOM

    dotnet restore $sharedProjectPath --source $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh shared NuGet consumer restore failed.'
    }

    dotnet build $sharedProjectPath -c Release --no-restore
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh shared NuGet consumer build failed.'
    }

    $dashboardConsumerDirectory = Join-Path $consumerRoot 'Dashboard'
    New-Item -ItemType Directory -Force -Path $dashboardConsumerDirectory | Out-Null
    $dashboardProjectPath = Join-Path $dashboardConsumerDirectory 'PackageConsumer.csproj'
    $dashboardProgramPath = Join-Path $dashboardConsumerDirectory 'Program.cs'

    @"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
    <PackageReference Include="MqttZero.AspNetCore.Dashboard" Version="$packageVersion" />
  </ItemGroup>
</Project>
"@ | Set-Content -LiteralPath $dashboardProjectPath -Encoding utf8NoBOM

    @'
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MqttZero.AspNetCore;
using MqttZero.AspNetCore.Dashboard;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddMqttZero();
builder.Services.AddMqttZeroDashboard(options =>
{
    options.ReadAuthorizationPolicy = "Read";
    options.OperateAuthorizationPolicy = "Operate";
});
WebApplication app = builder.Build();
app.MapMqttZero("/mqtt");
app.MapMqttZeroDashboard("/mqttzero");
'@ | Set-Content -LiteralPath $dashboardProgramPath -Encoding utf8NoBOM

    dotnet restore $dashboardProjectPath --source $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh NuGet consumer restore failed.'
    }

    dotnet build $dashboardProjectPath -c Release --no-restore
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh NuGet consumer build failed.'
    }
}
finally {
    Pop-Location
}
