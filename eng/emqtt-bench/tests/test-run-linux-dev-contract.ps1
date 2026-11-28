$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$script = Join-Path $root 'run-linux-dev.ps1'
$output = & $script -DryRun `
    -Mode throughput `
    -Qos 1 `
    -Subscribers 500 `
    -Payload 65536 `
    -Repetitions 1

if ($LASTEXITCODE -ne 0) {
    throw "Dry run failed."
}

$required = @(
    'hostId=linux-dev-2vcpu',
    'mode=throughput',
    'qos=1',
    'subscribers=500',
    'payload=65536',
    'repetitions=1'
)

foreach ($value in $required) {
    if ($output -notcontains $value) {
        throw "缺少 dry-run 输出：$value"
    }
}
