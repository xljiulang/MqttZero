$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repositoryRoot
try {
    dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
        -c Release --no-restore -- `
        --filter-method '*DoesNotAllocateAfterWarmup*' `
        --filter-method '*WithoutAllocation*'
    if ($LASTEXITCODE -ne 0) {
        throw "Allocation sentinel tests failed."
    }
}
finally {
    Pop-Location
}
