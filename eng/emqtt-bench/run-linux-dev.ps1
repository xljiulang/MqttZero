[CmdletBinding()]
param(
    [string]$SshConfigPath = $env:MQTTZERO_SSH_CONFIG,
    [ValidateSet('throughput', 'latency')]
    [string]$Mode = 'throughput',
    [string]$Qos = '0,1,2',
    [string]$Subscribers = '1,100,500',
    [string]$Payload = '32,8192,65536',
    [ValidateRange(1, 20)]
    [int]$Repetitions = 2,
    [ValidateRange(1, 65535)]
    [int]$StartPort = 19000,
    [ValidateRange(0, 1000000)]
    [int]$SmokeMessages = 0,
    [string]$Campaign = (Get-Date -Format 'yyyyMMdd-HHmmss'),
    [switch]$Quick,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$hostId = 'linux-dev-2vcpu'
$Mode = $Mode.Trim()
$Qos = $Qos.Trim()
$Subscribers = $Subscribers.Trim()
$Payload = $Payload.Trim()

if ($Qos -notmatch '^[0-9,]+$') {
    throw 'Qos 只能是逗号分隔的数字列表。'
}
if ($Subscribers -notmatch '^[0-9,]+$') {
    throw 'Subscribers 只能是逗号分隔的数字列表。'
}
if ($Payload -notmatch '^[0-9,]+$') {
    throw 'Payload 只能是逗号分隔的数字列表。'
}
if ($Quick) {
    if ($Mode -ne 'throughput') {
        throw 'Quick 模式只支持吞吐档。'
    }
    $Repetitions = 1
}

if ($DryRun) {
    'hostId=linux-dev-2vcpu'
    "mode=$Mode"
    "qos=$Qos"
    "subscribers=$Subscribers"
    "payload=$Payload"
    "repetitions=$Repetitions"
    "startPort=$StartPort"
    "smokeMessages=$SmokeMessages"
    "campaign=$Campaign"
    "quick=$($Quick.IsPresent)"
    exit 0
}

if ([string]::IsNullOrWhiteSpace($SshConfigPath)) {
    throw '必须通过 -SshConfigPath 或 MQTTZERO_SSH_CONFIG 指定 SSH 配置。'
}
if (-not (Test-Path -LiteralPath $SshConfigPath -PathType Leaf)) {
    throw "SSH 配置不存在：$SshConfigPath"
}
if ($Campaign -notmatch '^[A-Za-z0-9._-]+$') {
    throw 'Campaign 只能包含字母、数字、点、下划线和连字符。'
}
if ($SmokeMessages -gt 0 -and
    ($Qos -match ',' -or $Subscribers -match ',' -or $Payload -match ',')) {
    throw 'SmokeMessages 模式只允许单个 QoS、订阅者数和 Payload。'
}

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Push-Location $repoRoot
try {
    $status = @(git status --porcelain)
    if ($LASTEXITCODE -ne 0) {
        throw '无法读取 Git 工作树状态。'
    }
    if ($status.Count -gt 0) {
        throw "远程性能测试要求工作树干净：`n$($status -join "`n")"
    }

    $commit = (git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-f]{40}$') {
        throw '无法解析当前提交 SHA。'
    }
} finally {
    Pop-Location
}

$config = Get-Content -LiteralPath $SshConfigPath -Raw | ConvertFrom-Json
$hostName = [string]$config.ssh.host
$port = [int]$config.ssh.port
$user = [string]$config.ssh.user
$identityFile = [string]$config.ssh.authentication.identityFile
$remoteWorkdir = [string]$config.remote.workdir

if (-not (Test-Path -LiteralPath $identityFile -PathType Leaf)) {
    throw "SSH 私钥不存在：$identityFile"
}

$target = "$user@$hostName"
$remoteCampaign = "$remoteWorkdir/campaigns/$Campaign"
$localCampaign = Join-Path $repoRoot "artifacts/emqtt-bench/$Campaign"
$tempRoot = Join-Path ([IO.Path]::GetTempPath()) "mqttzero-$([Guid]::NewGuid())"
$archivePath = Join-Path $tempRoot 'source.tar.gz'

$sshArguments = @(
    '-o', 'BatchMode=yes',
    '-o', 'StrictHostKeyChecking=yes',
    '-i', $identityFile,
    '-p', $port.ToString(),
    $target
)
$scpArguments = @(
    '-q',
    '-o', 'BatchMode=yes',
    '-o', 'StrictHostKeyChecking=yes',
    '-i', $identityFile,
    '-P', $port.ToString()
)

function Invoke-Native {
    param(
        [Parameter(Mandatory)]
        [string]$FilePath,
        [Parameter(Mandatory)]
        [string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath 失败，退出码 $LASTEXITCODE。"
    }
}

New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null
New-Item -ItemType Directory -Path $localCampaign -Force | Out-Null

try {
    Push-Location $repoRoot
    try {
        Invoke-Native git @(
            'archive',
            '--format=tar.gz',
            "--output=$archivePath",
            $commit
        )
    } finally {
        Pop-Location
    }

    Invoke-Native ssh ($sshArguments + "mkdir -p '$remoteCampaign'")
    Invoke-Native scp ($scpArguments + @(
        $archivePath,
        "${target}:$remoteCampaign/source.tar.gz"
    ))

    $remoteScript = @'
set -euo pipefail
campaign_root="__REMOTE_CAMPAIGN__"
source_root="$campaign_root/source"
publish_root="$campaign_root/publish"
results_root="$campaign_root/results"
host_profile="$campaign_root/host.json"

test ! -e "$source_root"
mkdir -p "$source_root"
tar -xzf "$campaign_root/source.tar.gz" -C "$source_root"
cd "$source_root"

export MQTTZERO_BENCH_COMMIT="__COMMIT__"
export HOST_PROFILE="$host_profile"
export MQTTZERO_BROKER_DIR="$publish_root"
export MQTTZERO_DEV_ROOT="__REMOTE_WORKDIR__"
export NUGET_HTTP_CACHE_PATH="$campaign_root/nuget-http-cache"
__BENCHMARK_ENV__

dotnet restore \
    benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj \
    --locked-mode
dotnet publish \
    benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj \
    -c Release \
    --self-contained false \
    --no-restore \
    -p:UseAppHost=false \
    -o "$publish_root"
dotnet build-server shutdown >/dev/null 2>&1 || true

python3 eng/emqtt-bench/host_preflight.py \
    --host-id "__HOST_ID__" \
    --expected-cpus 2 \
    --expected-min-memory-gib 3 \
    --expected-fd-limit 1048576 \
    --output "$host_profile"

SMOKE_MESSAGES="__SMOKE_MESSAGES__" \
REPETITIONS="__REPETITIONS__" \
START_PORT="__START_PORT__" \
bash eng/emqtt-bench/run-remote-campaign.sh \
    "__MODE__" \
    "$results_root" \
    "__QOS__" \
    "__SUBSCRIBERS__" \
    "__PAYLOAD__"
'@

    $remoteScript = $remoteScript.
        Replace('__REMOTE_CAMPAIGN__', $remoteCampaign).
        Replace('__COMMIT__', $commit).
        Replace('__REMOTE_WORKDIR__', $remoteWorkdir).
        Replace('__HOST_ID__', $hostId).
        Replace('__SMOKE_MESSAGES__', $SmokeMessages.ToString()).
        Replace('__REPETITIONS__', $Repetitions.ToString()).
        Replace('__START_PORT__', $StartPort.ToString()).
        Replace('__MODE__', $Mode).
        Replace('__QOS__', $Qos).
        Replace('__SUBSCRIBERS__', $Subscribers).
        Replace('__PAYLOAD__', $Payload)

    $forwardedNames = @(
        'MAX_BUFFERED_BYTES',
        'OUTBOUND_BATCH_BYTES',
        'OUTBOUND_BATCH_MESSAGES',
        'OUTBOUND_BATCH_DURATION_US',
        'EVENT_LOOP_SHARDS',
        'IO_QUEUE_COUNT',
        'ENABLE_FANOUT_DIAGNOSTICS',
        'ENABLE_MQTTZERO_MEMORY_PROBE',
        'MEMORY_PROBE_MS',
        'MAX_PENDING',
        'PROCESS_SAMPLE_MS',
        'QOE_FLUSH_SECONDS'
    )
    $forwardedLines = foreach ($name in $forwardedNames) {
        $value = [Environment]::GetEnvironmentVariable($name)
        if ($Quick -and $name -eq 'QOE_FLUSH_SECONDS') {
            $value = '1'
        }
        if ([string]::IsNullOrEmpty($value)) {
            continue
        }
        if ($value -notmatch '^[A-Za-z0-9._,-]+$') {
            throw "环境变量 $name 含有不支持的字符。"
        }
        "export $name='$value'"
    }
    $remoteScript = $remoteScript.Replace(
        '__BENCHMARK_ENV__',
        $forwardedLines -join "`n"
    )
    $remoteScript = $remoteScript.Replace("`r", '')

    $remoteScript | & ssh @sshArguments "tr -d '\r' | bash -s"
    if ($LASTEXITCODE -ne 0) {
        throw "Linux 开发服务器测试失败，退出码 $LASTEXITCODE。"
    }

    $remoteHostCheck = (
        "cd '$remoteCampaign/source' && " +
        "MQTTZERO_BENCH_COMMIT='$commit' " +
        "python3 eng/emqtt-bench/host_preflight.py " +
        "--host-id '$hostId' " +
        "--expected-cpus 2 " +
        "--expected-min-memory-gib 3 " +
        "--expected-fd-limit 1048576 " +
        "--output '$remoteCampaign/host-after.json' && " +
        "cmp '$remoteCampaign/host.json' " +
        "'$remoteCampaign/host-after.json'"
    )
    Invoke-Native ssh ($sshArguments + $remoteHostCheck)

    Invoke-Native scp ($scpArguments + @(
        '-r',
        "${target}:$remoteCampaign/results",
        $localCampaign
    ))
    Invoke-Native scp ($scpArguments + @(
        "${target}:$remoteCampaign/host.json",
        "${target}:$remoteCampaign/host-after.json",
        $localCampaign
    ))

    $remoteCleanup = (
        "case '$remoteCampaign' in " +
        "'$remoteWorkdir'/campaigns/*) " +
        "rm -rf -- '$remoteCampaign/source' " +
        "'$remoteCampaign/publish' " +
        "'$remoteCampaign/source.tar.gz' " +
        "'$remoteCampaign/nuget-http-cache' ;; " +
        "*) exit 1 ;; esac"
    )
    Invoke-Native ssh ($sshArguments + $remoteCleanup)
} finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output "campaign=$Campaign"
Write-Output "results=$localCampaign"
