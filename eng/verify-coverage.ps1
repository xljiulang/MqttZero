param(
    [string] $Configuration = 'Release'
)

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$coverageDirectory = Join-Path $repositoryRoot 'artifacts/coverage'
$settingsPath = Join-Path $repositoryRoot 'eng/coverage.config'
$brokerReportPath = Join-Path $coverageDirectory 'broker.cobertura.xml'
$dashboardReportPath = Join-Path $coverageDirectory 'dashboard.cobertura.xml'
$summaryPath = Join-Path $coverageDirectory 'summary.md'
$brokerProject = Join-Path $repositoryRoot 'tests/MqttZero.Tests/MqttZero.Tests.csproj'
$dashboardProject = Join-Path `
    $repositoryRoot `
    'tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj'

$thresholds = @(
    [pscustomobject] @{
        Package = 'MqttZero'
        Report = $brokerReportPath
        Line = 0.86
        Branch = 0.70
    },
    [pscustomobject] @{
        Package = 'MqttZero.Server'
        Report = $brokerReportPath
        Line = 0.80
        Branch = 0.72
    },
    [pscustomobject] @{
        Package = 'MqttZero.AspNetCore'
        Report = $brokerReportPath
        Line = 0.90
        Branch = 0.85
    },
    [pscustomobject] @{
        Package = 'MqttZero.AspNetCore.Dashboard'
        Report = $dashboardReportPath
        Line = 0.80
        Branch = 0.70
    }
)

New-Item -ItemType Directory -Force -Path $coverageDirectory | Out-Null
Remove-Item -LiteralPath $brokerReportPath -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $dashboardReportPath -Force -ErrorAction SilentlyContinue

dotnet test --project $brokerProject `
    -c $Configuration `
    --no-build `
    -- `
    --coverage `
    --coverage-settings $settingsPath `
    --coverage-output $brokerReportPath `
    --coverage-output-format cobertura

if ($LASTEXITCODE -ne 0)
{
    throw 'Broker coverage run failed.'
}

dotnet test --project $dashboardProject `
    -c $Configuration `
    --no-build `
    -- `
    --coverage `
    --coverage-settings $settingsPath `
    --coverage-output $dashboardReportPath `
    --coverage-output-format cobertura

if ($LASTEXITCODE -ne 0)
{
    throw 'Dashboard coverage run failed.'
}

$failures = [System.Collections.Generic.List[string]]::new()
$summaryLines = [System.Collections.Generic.List[string]]::new()
$summaryLines.Add('# Coverage Summary')
$summaryLines.Add('')
$summaryLines.Add('| Package | Line | Branch | Line minimum | Branch minimum | Result |')
$summaryLines.Add('|---|---:|---:|---:|---:|---|')

foreach ($threshold in $thresholds)
{
    [xml] $document = Get-Content -Raw -LiteralPath $threshold.Report
    $package = $document.coverage.packages.package |
        Where-Object { $_.name -eq $threshold.Package } |
        Select-Object -First 1
    if ($package -eq $null)
    {
        $failures.Add("Package $($threshold.Package) was not found in $($threshold.Report).")
        $summaryLines.Add(
            "| $($threshold.Package) | n/a | n/a | " +
            "$([math]::Round($threshold.Line * 100, 2))% | " +
            "$([math]::Round($threshold.Branch * 100, 2))% | FAIL |")
        continue
    }

    [double] $lineRate = $package.'line-rate'
    [double] $branchRate = $package.'branch-rate'
    $passed = $lineRate -ge $threshold.Line -and
        $branchRate -ge $threshold.Branch
    $result = if ($passed) { 'PASS' } else { 'FAIL' }
    $linePercent = [math]::Round($lineRate * 100, 2)
    $branchPercent = [math]::Round($branchRate * 100, 2)
    $summaryLines.Add(
        "| $($threshold.Package) | $linePercent% | $branchPercent% | " +
        "$([math]::Round($threshold.Line * 100, 2))% | " +
        "$([math]::Round($threshold.Branch * 100, 2))% | $result |")

    if (-not $passed)
    {
        $failures.Add(
            "$($threshold.Package) coverage is $linePercent% line / " +
            "$branchPercent% branch; expected at least " +
            "$([math]::Round($threshold.Line * 100, 2))% / " +
            "$([math]::Round($threshold.Branch * 100, 2))%.")
    }
}

$summaryLines | Set-Content -LiteralPath $summaryPath -Encoding utf8NoBOM
$summaryLines | Write-Output

if ($failures.Count -gt 0)
{
    throw ($failures -join [Environment]::NewLine)
}
