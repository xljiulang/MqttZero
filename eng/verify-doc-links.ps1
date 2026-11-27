[CmdletBinding()]
param(
    [string] $Root = ''
)

$ErrorActionPreference = 'Stop'

$repoRoot = if ([string]::IsNullOrWhiteSpace($Root)) {
    Split-Path -Parent $PSScriptRoot
}
else {
    (Resolve-Path -LiteralPath $Root).Path
}

$documents = Get-ChildItem -LiteralPath $repoRoot -Recurse -File -Filter '*.md' |
    Where-Object {
        $path = $_.FullName.Replace('\', '/')
        $path -notmatch '/(bin|obj|artifacts|node_modules)/'
    }

$linkPattern = [regex]'\]\((?<target>[^)\s]+)\)'
$failures = [System.Collections.Generic.List[string]]::new()
$checked = 0

foreach ($document in $documents) {
    $text = [System.IO.File]::ReadAllText($document.FullName)
    foreach ($match in $linkPattern.Matches($text)) {
        $target = $match.Groups['target'].Value
        if ($target.StartsWith('#') -or
            $target.StartsWith('http://') -or
            $target.StartsWith('https://') -or
            $target.StartsWith('mailto:')) {
            continue
        }

        $relative = $target.Split('#')[0]
        if ($relative.Length -eq 0) {
            continue
        }

        $checked++
        $resolved = Join-Path $document.DirectoryName $relative
        if (-not (Test-Path -LiteralPath $resolved)) {
            $display = [System.IO.Path]::GetRelativePath($repoRoot, $document.FullName)
            $failures.Add("$display -> $target")
        }
    }
}

if ($failures.Count -gt 0) {
    Write-Output "Broken documentation links:"
    foreach ($failure in $failures) {
        Write-Output "  $failure"
    }

    exit 1
}

Write-Output "Documentation links verified: $checked targets in $($documents.Count) files."
