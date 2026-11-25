param(
    [ValidateSet('mqttnet', 'raw')]
    [string] $Client = 'mqttnet',
    [string] $OutputPath = 'artifacts/broker-baseline.json',
    [int] $StartPort = 30000,
    [string] $Qos = '0,1',
    [string] $Subscribers = '1,100',
    [string] $PayloadSizes = '32,8192,65536',
    [int] $WarmupSeconds = 2,
    [int] $MeasurementSeconds = 6,
    [int] $Repetitions = 1
)

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$benchmarkProject = Join-Path `
    $repositoryRoot `
    'benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj'
$candidateOutputPath = if ([System.IO.Path]::IsPathRooted($OutputPath))
{
    $OutputPath
}
else
{
    Join-Path $repositoryRoot $OutputPath
}
$resolvedOutputPath = [System.IO.Path]::GetFullPath($candidateOutputPath)
$outputDirectory = Split-Path -Parent $resolvedOutputPath

New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
Remove-Item -LiteralPath $resolvedOutputPath -Force -ErrorAction SilentlyContinue

if ($Client -eq 'raw' -and $Qos.Split(',') -contains '2')
{
    $Qos = '0,1'
    Write-Output 'Raw client selected; QoS 2 is excluded from the diagnostic matrix.'
}

& dotnet run `
    -c Release `
    --no-build `
    --project $benchmarkProject `
    -- compare `
    --client $Client `
    --qos $Qos `
    --subscribers $Subscribers `
    --payload $PayloadSizes `
    --warmup $WarmupSeconds `
    --seconds $MeasurementSeconds `
    --repetitions $Repetitions `
    --max-pending 1024 `
    --start-port $StartPort `
    --output $resolvedOutputPath
$exitCode = $LASTEXITCODE

if (-not (Test-Path -LiteralPath $resolvedOutputPath))
{
    throw "Broker benchmark did not produce $resolvedOutputPath."
}

$document = Get-Content -Raw -LiteralPath $resolvedOutputPath | ConvertFrom-Json
if ($null -eq $document.Gate)
{
    throw "Broker benchmark output does not contain Gate."
}

Write-Output "Broker benchmark output: $resolvedOutputPath"
Write-Output "Gate passed: $($document.Gate.Passed)"
exit $exitCode
