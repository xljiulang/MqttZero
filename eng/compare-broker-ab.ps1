[CmdletBinding()]
param(
    [string] $BaselinePath = '',
    [string] $BaselineRef = 'HEAD~1',
    [string] $CandidatePath = '',
    [int[]] $Qos = @(1),
    [int[]] $Subscribers = @(100),
    [int[]] $PayloadSizes = @(65536),
    [int] $Rounds = 3,
    [int] $WarmupSeconds = 5,
    [int] $MeasurementSeconds = 20,
    [int] $MaxPending = 1024,
    [int] $MinGainPercent = 10,
    [int] $MaxDeliveryP99RegressionPercent = 5,
    [int] $DeliveryP99AbsoluteUs = 0,
    [int] $StartPort = 25000,
    [string] $OutputDirectory = 'artifacts/broker-ab',
    [switch] $SkipBuild,
    [switch] $SkipExisting,
    [switch] $Diagnostics
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

function Resolve-Checkout([string] $path) {
    if ([string]::IsNullOrWhiteSpace($path)) {
        return $repoRoot
    }

    return (Resolve-Path -LiteralPath $path).Path
}

function Get-CheckoutCommit([string] $path) {
    $commit = (& git -C $path rev-parse --short HEAD)
    if ($LASTEXITCODE -ne 0) {
        throw "Could not resolve the commit of '$path'."
    }

    return $commit.Trim()
}

function Build-BenchmarkAssembly([string] $path) {
    $project = Join-Path `
        $path `
        'benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj'
    & dotnet restore $project --locked-mode 2>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Locked restore failed for '$path'."
    }

    & dotnet build $project -c Release --no-restore 2>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Release build failed for '$path'."
    }

    return Join-Path `
        $path `
        'benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll'
}

function Get-Median([double[]] $values) {
    if ($values.Count -eq 0) {
        return 0
    }

    $ordered = @($values | Sort-Object)
    $middle = [int][math]::Floor($ordered.Count / 2)
    if ($ordered.Count % 2 -eq 0) {
        return ($ordered[$middle - 1] + $ordered[$middle]) / 2
    }

    return $ordered[$middle]
}

$candidatePath = Resolve-Checkout $CandidatePath
if ([string]::IsNullOrWhiteSpace($BaselinePath)) {
    $worktreeRoot = Join-Path $repoRoot '.worktrees'
    $baselinePath = Join-Path $worktreeRoot "ab-$BaselineRef"
    if (-not (Test-Path -LiteralPath $baselinePath)) {
        New-Item -ItemType Directory -Force -Path $worktreeRoot | Out-Null
        & git -C $repoRoot worktree add $baselinePath $BaselineRef
        if ($LASTEXITCODE -ne 0) {
            throw "Could not create the baseline worktree at '$baselinePath'."
        }
    }
}
else {
    $baselinePath = Resolve-Checkout $BaselinePath
}

$outputRoot = if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    $OutputDirectory
}
else {
    Join-Path $repoRoot $OutputDirectory
}

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

$baselineCommit = Get-CheckoutCommit $baselinePath
$candidateCommit = Get-CheckoutCommit $candidatePath
Write-Output "Baseline: $baselinePath ($baselineCommit)"
Write-Output "Candidate: $candidatePath ($candidateCommit)"
Write-Output "Output: $outputRoot"

if ($SkipBuild) {
    $baselineAssembly = Join-Path `
        $baselinePath `
        'benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll'
    $candidateAssembly = Join-Path `
        $candidatePath `
        'benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll'
}
else {
    $baselineAssembly = Build-BenchmarkAssembly $baselinePath
    $candidateAssembly = Build-BenchmarkAssembly $candidatePath
}

$driverAssembly = $candidateAssembly
$diagnosticArguments = @()
if ($Diagnostics) {
    $diagnosticArguments = @('--diagnostics', 'true')
}

$cellIndex = 0
$cells = @()
foreach ($cellQos in $Qos) {
    foreach ($cellSubscribers in $Subscribers) {
        foreach ($cellPayload in $PayloadSizes) {
            $cellIndex++
            $cellName = "qos$cellQos-subs$cellSubscribers-payload$cellPayload"
            $cellPath = Join-Path $outputRoot "$cellName.json"
            if ($SkipExisting -and (Test-Path -LiteralPath $cellPath)) {
                Write-Output "Skipping existing cell $cellName"
                $cells += Get-Content -Raw -LiteralPath $cellPath | ConvertFrom-Json
                continue
            }

            $cellPort = $StartPort + ($cellIndex * (($Rounds * 2) + 2))
            Write-Output (
                "Running cell $cellName " +
                "(qos=$cellQos subscribers=$cellSubscribers " +
                "payload=$cellPayload rounds=$Rounds port=$cellPort)")
            & dotnet $driverAssembly `
                ab `
                --baseline-broker $baselineAssembly `
                --candidate-broker $candidateAssembly `
                --baseline-commit $baselineCommit `
                --candidate-commit $candidateCommit `
                --qos $cellQos `
                --subscribers $cellSubscribers `
                --payload $cellPayload `
                --publishers 1 `
                --warmup $WarmupSeconds `
                --seconds $MeasurementSeconds `
                --rounds $Rounds `
                --max-pending $MaxPending `
                --start-port $cellPort `
                --min-gain-percent $MinGainPercent `
                --max-delivery-p99-regression-percent $MaxDeliveryP99RegressionPercent `
                --delivery-p99-absolute-us $DeliveryP99AbsoluteUs `
                --output $cellPath `
                @diagnosticArguments
            if (-not (Test-Path -LiteralPath $cellPath)) {
                throw "Cell '$cellName' did not produce $cellPath."
            }

            $cells += Get-Content -Raw -LiteralPath $cellPath | ConvertFrom-Json
        }
    }
}

$results = @()
$anyFailed = $false
foreach ($cell in $cells) {
    $summary = $cell.Summary
    $isTarget = (
        $cell.Qos -eq 1 -and
        $cell.Subscribers -eq 100 -and
        $cell.PayloadSize -eq 65536)
    $passed = [bool]$summary.Passed
    if (-not $passed) {
        $anyFailed = $true
    }

    $baselineRounds = @($cell.Rounds | Where-Object { $_.Variant -eq 'baseline' })
    $candidateRounds = @($cell.Rounds | Where-Object { $_.Variant -eq 'candidate' })
    $results += [pscustomobject]@{
        Cell = "qos$($cell.Qos)-subs$($cell.Subscribers)-payload$($cell.PayloadSize)"
        Target = $isTarget
        BaselineThroughput = [math]::Round($summary.BaselineEndToEndThroughput, 2)
        CandidateThroughput = [math]::Round($summary.CandidateEndToEndThroughput, 2)
        ThroughputChange = [math]::Round($summary.ThroughputChangeRatio, 4)
        BaselineDeliveryP99 = [math]::Round($summary.BaselineDeliveryP99Microseconds, 1)
        CandidateDeliveryP99 = [math]::Round($summary.CandidateDeliveryP99Microseconds, 1)
        DeliveryP99Ratio = [math]::Round($summary.DeliveryP99Ratio, 3)
        ThroughputMetric = $summary.ThroughputMetric
        DrainRatio = [math]::Round($summary.DrainRatio, 3)
        BaselineBrokerCpuMs = [math]::Round((Get-Median @($baselineRounds |
            ForEach-Object { $_.BrokerObservation.TotalProcessorMilliseconds })), 1)
        CandidateBrokerCpuMs = [math]::Round((Get-Median @($candidateRounds |
            ForEach-Object { $_.BrokerObservation.TotalProcessorMilliseconds })), 1)
        BaselineClientCpuMs = [math]::Round((Get-Median @($baselineRounds |
            ForEach-Object { $_.ClientObservation.TotalProcessorMilliseconds })), 1)
        CandidateClientCpuMs = [math]::Round((Get-Median @($candidateRounds |
            ForEach-Object { $_.ClientObservation.TotalProcessorMilliseconds })), 1)
        BaselineBrokerPeakWorkingSetMb = [math]::Round((Get-Median @($baselineRounds |
            ForEach-Object { $_.BrokerObservation.PeakWorkingSetBytes })) / 1MB, 1)
        CandidateBrokerPeakWorkingSetMb = [math]::Round((Get-Median @($candidateRounds |
            ForEach-Object { $_.BrokerObservation.PeakWorkingSetBytes })) / 1MB, 1)
        Failures = $summary.Failures
        Integrity = (
            "missing=$($summary.MissingDeliveryMessages) " +
            "regressions=$($summary.SequenceRegressions) " +
            "duplicates=$($summary.DuplicateDeliveryMessages)")
        Passed = $passed
    }
}

$summaryPath = Join-Path $outputRoot 'summary.json'
$results | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $summaryPath -Encoding utf8

$markdownPath = Join-Path $outputRoot 'summary.md'
$header = @(
    "| Cell | Target | Baseline msg/s | Candidate msg/s | Change | Baseline p99 us | Candidate p99 us | Ratio | Broker CPU ms (base/cand) | Client CPU ms (base/cand) | Broker peak WS MB (base/cand) | Failures | Integrity | Passed |"
    "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|"
)
$rows = $results | ForEach-Object {
    "| $($_.Cell) | $($_.Target) | $($_.BaselineThroughput) | " +
        "$($_.CandidateThroughput) | $('{0:P1}' -f $_.ThroughputChange) | " +
        "$($_.BaselineDeliveryP99) | $($_.CandidateDeliveryP99) | " +
        "$($_.DeliveryP99Ratio) | " +
        "$($_.BaselineBrokerCpuMs)/$($_.CandidateBrokerCpuMs) | " +
        "$($_.BaselineClientCpuMs)/$($_.CandidateClientCpuMs) | " +
        "$($_.BaselineBrokerPeakWorkingSetMb)/$($_.CandidateBrokerPeakWorkingSetMb) | " +
        "$($_.Failures) | $($_.Integrity) | $($_.Passed) |"
}
$header + $rows | Set-Content -LiteralPath $markdownPath -Encoding utf8

Write-Output "Summary: $markdownPath"
$results | Format-Table -AutoSize | Out-String | Write-Output

if ($anyFailed) {
    exit 1
}

exit 0
