param(
    [int] $StartPort = 25000,
    [string] $OutputDirectory = 'artifacts/benchmark-smoke'
)

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$outputRoot = Join-Path $repositoryRoot $OutputDirectory
$benchmarkProject = Join-Path `
    $repositoryRoot `
    'benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj'

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

$mqttNetOutput = Join-Path $outputRoot 'mqttnet-client.json'
$rawOutput = Join-Path $outputRoot 'raw-client.json'
Remove-Item -LiteralPath $mqttNetOutput -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $rawOutput -Force -ErrorAction SilentlyContinue

function Invoke-BenchmarkSmoke {
    param(
        [Parameter(Mandatory)]
        [string] $Client,

        [Parameter(Mandatory)]
        [int] $Port,

        [Parameter(Mandatory)]
        [string] $OutputPath
    )

    & dotnet run `
        -c Release `
        --no-build `
        --project $benchmarkProject `
        -- compare `
        --client $Client `
        --qos 0,1 `
        --subscribers 1 `
        --payload 32,8192 `
        --warmup 1 `
        --seconds 2 `
        --repetitions 1 `
        --max-pending 1024 `
        --start-port $Port `
        --output $OutputPath

    if (-not (Test-Path -LiteralPath $OutputPath))
    {
        throw "Benchmark smoke did not produce $OutputPath."
    }

    $document = Get-Content -Raw -LiteralPath $OutputPath | ConvertFrom-Json
    if ($null -eq $document.Results)
    {
        throw "Benchmark smoke output $OutputPath does not contain Results."
    }

    foreach ($broker in @('mqttzero', 'mqttnet'))
    {
        $brokerResults = @(
            $document.Results |
                Where-Object { $_.Broker -eq $broker }
        )
        if ($brokerResults.Count -eq 0)
        {
            throw "Benchmark smoke output $OutputPath is missing $broker."
        }

        $failures = (
            $brokerResults |
                ForEach-Object { $_.Result.FailedOperations } |
                Measure-Object -Sum
        ).Sum
        if ($failures -ne 0)
        {
            throw "Benchmark smoke for $broker reported $failures failures."
        }
    }

    Write-Output "Benchmark smoke passed for $Client."
}

Invoke-BenchmarkSmoke `
    -Client 'mqttnet' `
    -Port $StartPort `
    -OutputPath $mqttNetOutput
Invoke-BenchmarkSmoke `
    -Client 'raw' `
    -Port ($StartPort + 1000) `
    -OutputPath $rawOutput
