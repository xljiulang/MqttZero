param(
    [int[]] $EventLoopShards = @(1, 2, 4, 8),
    [int[]] $IoQueueCounts = @(1, 2, 4, 8),
    [string] $OutputDirectory = 'artifacts/topology',
    [int] $StartPort = 40000
)

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$project = Join-Path `
    $repositoryRoot `
    'benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj'
$resolvedOutputDirectory = if ([System.IO.Path]::IsPathRooted(
        $OutputDirectory))
{
    $OutputDirectory
}
else
{
    Join-Path $repositoryRoot $OutputDirectory
}

New-Item `
    -ItemType Directory `
    -Force `
    -Path $resolvedOutputDirectory | Out-Null

$rows = [System.Collections.Generic.List[object]]::new()
$port = $StartPort
foreach ($shards in $EventLoopShards)
{
    foreach ($ioQueues in $IoQueueCounts)
    {
        $port++
        $outputPath = Join-Path `
            $resolvedOutputDirectory `
            "qos0-100-8k-s$shards-io$ioQueues.json"
        & dotnet run `
            -c Release `
            --no-build `
            --project $project `
            -- compare `
            --client mqttnet `
            --qos 0 `
            --subscribers 100 `
            --payload 8192 `
            --warmup 2 `
            --seconds 6 `
            --repetitions 1 `
            --max-pending 1024 `
            --start-port $port `
            --event-loop-shards $shards `
            --io-queue-count $ioQueues `
            --output $outputPath
        if (-not (Test-Path -LiteralPath $outputPath))
        {
            throw "Topology benchmark did not write $outputPath."
        }

        $document = Get-Content -Raw -LiteralPath $outputPath |
            ConvertFrom-Json
        $mqttZero = @(
            $document.Results |
                Where-Object Broker -eq 'mqttzero'
        )[0].Result
        $rows.Add([pscustomobject] @{
            EventLoopShards = $shards
            IoQueueCount = $ioQueues
            Throughput = [math]::Round(
                [double] $mqttZero.ThroughputPerSecond,
                2)
            P99Microseconds = [math]::Round(
                [double] $mqttZero.LatencyP99Microseconds,
                1)
            Output = $outputPath
        })
    }
}

$rows |
    Sort-Object Throughput -Descending |
    Format-Table -AutoSize |
    Out-String -Width 240 |
    Write-Output
