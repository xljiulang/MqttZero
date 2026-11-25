param(
    [Parameter(Mandatory)]
    [string] $Baseline,

    [Parameter(Mandatory)]
    [string] $Current,

    [double] $MaxThroughputRegression = 0.05,
    [double] $MaxP99Regression = 0.05
)

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$baselinePath = if ([System.IO.Path]::IsPathRooted($Baseline))
{
    $Baseline
}
else
{
    Join-Path $repositoryRoot $Baseline
}
$currentPath = if ([System.IO.Path]::IsPathRooted($Current))
{
    $Current
}
else
{
    Join-Path $repositoryRoot $Current
}

$baselineDocument = Get-Content -Raw -LiteralPath $baselinePath |
    ConvertFrom-Json
$currentDocument = Get-Content -Raw -LiteralPath $currentPath |
    ConvertFrom-Json

function Get-MqttZeroCells {
    param(
        [Parameter(Mandatory)]
        [object] $Document
    )

    $groups = @{}
    foreach ($result in @($Document.Results))
    {
        if ($result.Broker -ne 'mqttzero')
        {
            continue
        }

        $key = "{0}|{1}|{2}" -f `
            $result.Qos, `
            $result.Subscribers, `
            $result.PayloadSize
        if (-not $groups.ContainsKey($key))
        {
            $groups[$key] = [System.Collections.Generic.List[object]]::new()
        }

        $groups[$key].Add($result.Result)
    }

    $cells = @{}
    foreach ($entry in $groups.GetEnumerator())
    {
        [double[]] $throughputs = @(
            $entry.Value |
                ForEach-Object { [double] $_.ThroughputPerSecond }
        )
        [double[]] $p99Values = @(
            $entry.Value |
                ForEach-Object { [double] $_.LatencyP99Microseconds }
        )
        $throughputs = $throughputs | Sort-Object
        $p99Values = $p99Values | Sort-Object
        $cells[$entry.Key] = [pscustomobject] @{
            Throughput = Get-Median -Values $throughputs
            P99 = Get-Median -Values $p99Values
        }
    }

    return $cells
}

function Get-Median {
    param(
        [Parameter(Mandatory)]
        [double[]] $Values
    )

    if ($Values.Count -eq 0)
    {
        return 0
    }

    $middle = [int] [math]::Floor($Values.Count / 2)
    if ($Values.Count % 2 -eq 0)
    {
        return ($Values[$middle - 1] + $Values[$middle]) / 2
    }

    return $Values[$middle]
}

$baselineCells = Get-MqttZeroCells -Document $baselineDocument
$currentCells = Get-MqttZeroCells -Document $currentDocument
$failures = [System.Collections.Generic.List[string]]::new()
$rows = [System.Collections.Generic.List[object]]::new()

foreach ($key in $baselineCells.Keys | Sort-Object)
{
    if (-not $currentCells.ContainsKey($key))
    {
        $failures.Add("Current benchmark is missing cell $key.")
        continue
    }

    $baselineCell = $baselineCells[$key]
    $currentCell = $currentCells[$key]
    $throughputRatio = $currentCell.Throughput / $baselineCell.Throughput
    $p99Ratio = $currentCell.P99 / $baselineCell.P99
    $throughputPassed =
        $throughputRatio -ge (1 - $MaxThroughputRegression)
    $p99Passed = $p99Ratio -le (1 + $MaxP99Regression)
    $passed = $throughputPassed -and $p99Passed
    if (-not $passed)
    {
        $failures.Add(
            "Cell $key regressed: throughput=$([math]::Round($throughputRatio, 3))x, p99=$([math]::Round($p99Ratio, 3))x.")
    }

    $rows.Add([pscustomobject] @{
        Cell = $key
        BaselineThroughput = [math]::Round($baselineCell.Throughput, 2)
        CurrentThroughput = [math]::Round($currentCell.Throughput, 2)
        ThroughputRatio = [math]::Round($throughputRatio, 3)
        BaselineP99Us = [math]::Round($baselineCell.P99, 1)
        CurrentP99Us = [math]::Round($currentCell.P99, 1)
        P99Ratio = [math]::Round($p99Ratio, 3)
        Passed = $passed
    })
}

$rows | Format-Table -AutoSize | Out-String -Width 240 | Write-Output

if ($failures.Count -gt 0)
{
    $failures | Write-Output
    exit 1
}

Write-Output 'Phase 1 broker regression gate passed.'
