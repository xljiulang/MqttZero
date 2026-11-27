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

$outputPath = Join-Path $outputRoot 'raw-client.json'
Remove-Item -LiteralPath $outputPath -Force -ErrorAction SilentlyContinue

function Invoke-BenchmarkSmoke {
    param(
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
        --qos 0,1 `
        --subscribers 1 `
        --payload 32,8192 `
        --warmup 1 `
        --seconds 2 `
        --repetitions 1 `
        --max-pending 64 `
        --comparison-gate false `
        --start-port $Port `
        --output $OutputPath

    if ($LASTEXITCODE -ne 0)
    {
        Write-BenchmarkFailureAnnotations `
            -OutputPath $OutputPath
        throw (
            "Benchmark smoke exited with code " +
            "$LASTEXITCODE. Inspect $OutputPath.")
    }

    if (-not (Test-Path -LiteralPath $OutputPath))
    {
        Write-Output (
            "::error title=Benchmark smoke::" +
            "Benchmark smoke did not produce $OutputPath.")
        throw "Benchmark smoke did not produce $OutputPath."
    }

    $document = Get-Content -Raw -LiteralPath $OutputPath | ConvertFrom-Json
    if ($null -eq $document.Results)
    {
        Write-Output (
            "::error title=Benchmark smoke::" +
            "Benchmark smoke output $OutputPath does not contain Results.")
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
            Write-Output (
                "::error title=Benchmark smoke::" +
                "Benchmark smoke output $OutputPath is missing $broker.")
            throw "Benchmark smoke output $OutputPath is missing $broker."
        }

        $failures = (
            $brokerResults |
                ForEach-Object { $_.Result.FailedOperations } |
                Measure-Object -Sum
        ).Sum
        if ($failures -ne 0)
        {
            Write-BenchmarkFailureAnnotations `
                -OutputPath $OutputPath
            Write-Output (
                "::error title=Benchmark smoke::" +
                "Benchmark smoke for $broker reported $failures failures.")
            throw "Benchmark smoke for $broker reported $failures failures."
        }
    }

    Write-Output 'Benchmark smoke passed.'
}

function Write-BenchmarkFailureAnnotations {
    param(
        [Parameter(Mandatory)]
        [string] $OutputPath
    )

    if (-not (Test-Path -LiteralPath $OutputPath))
    {
        Write-Output (
            "::error title=Benchmark smoke::" +
            "Benchmark smoke did not write $OutputPath.")
        return
    }

    $document = Get-Content -Raw -LiteralPath $OutputPath | ConvertFrom-Json
    if ($null -eq $document.Results)
    {
        Write-Output (
            "::error title=Benchmark smoke::" +
            "Benchmark smoke output has no Results.")
        return
    }

    foreach ($comparison in $document.Results)
    {
        $result = $comparison.Result
        if ($result.FailedOperations -eq 0 -and
            $result.MissingDeliveryMessages -eq 0 -and
            $result.DuplicateDeliveryMessages -eq 0 -and
            $result.SequenceRegressions -eq 0 -and
            $result.UnsequencedDeliveryMessages -eq 0)
        {
            continue
        }

        Write-Output (
            "::error title=Benchmark delivery::" +
            "broker=$($comparison.Broker) " +
            "qos=$($comparison.Qos) " +
            "payload=$($comparison.PayloadSize) " +
            "failed=$($result.FailedOperations) " +
            "missing=$($result.MissingDeliveryMessages) " +
            "duplicates=$($result.DuplicateDeliveryMessages) " +
            "regressions=$($result.SequenceRegressions) " +
            "unsequenced=$($result.UnsequencedDeliveryMessages)")
    }

    if ($null -eq $document.Gate)
    {
        return
    }

    if (-not $document.Gate.Passed)
    {
        Write-Output (
            "::error title=Benchmark gate::" +
            "Benchmark comparison gate failed.")
    }

    foreach ($summary in $document.Gate.Summaries)
    {
        if ($summary.Passed)
        {
            continue
        }

        Write-Output (
            "::error title=Benchmark gate::" +
            "qos=$($summary.Qos) " +
            "payload=$($summary.PayloadSize) " +
            "throughput-ratio=$($summary.ThroughputRatio.ToString('F3')) " +
            "p99-ratio=$($summary.P99Ratio.ToString('F3')) " +
            "reason=$($summary.FailureReason)")
    }
}

try {
    Invoke-BenchmarkSmoke `
        -Port $StartPort `
        -OutputPath $outputPath
}
catch {
    Write-Output "::error title=Benchmark smoke::$($_.Exception.Message)"
    throw
}
