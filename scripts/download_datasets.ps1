param(
    [string]$OutputDir = "data/raw",
    [int]$MaxRows = 0,
    [switch]$UseApi,
    [string]$AppToken = $env:SOCRATA_APP_TOKEN,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$datasets = @(
    @{ Name = "crimes"; Id = "ijzp-q8t2"; File = "crimes_2001_to_present.csv" },
    @{ Name = "police_stations"; Id = "z8bn-74gv"; File = "police_stations.csv" },
    @{ Name = "arrests"; Id = "dpt3-jri9"; File = "arrests.csv" },
    @{ Name = "violence_reduction"; Id = "gumc-mgzr"; File = "violence_reduction_victims.csv" },
    @{ Name = "sex_offenders"; Id = "vc9r-bqvy"; File = "sex_offenders.csv" }
)

if (-not (Test-Path -LiteralPath $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

function Get-Url {
    param(
        [string]$DatasetId,
        [int]$Rows,
        [bool]$ApiMode
    )

    if ($ApiMode) {
        $base = "https://data.cityofchicago.org/resource/$DatasetId.csv"
        if ($Rows -gt 0) {
            return "$($base)?`$limit=$Rows"
        }
        return $base
    }

    return "https://data.cityofchicago.org/api/views/$DatasetId/rows.csv?accessType=DOWNLOAD"
}

$headers = @{}
if ($AppToken) {
    $headers["X-App-Token"] = $AppToken
}

Write-Host "Downloading City of Chicago datasets..." -ForegroundColor Cyan
Write-Host "OutputDir: $OutputDir"
if ($UseApi) {
    Write-Host "Mode: API CSV"
}
else {
    Write-Host "Mode: Export CSV"
}
if ($MaxRows -gt 0) {
    Write-Host "MaxRows: $MaxRows"
}

foreach ($d in $datasets) {
    $url = Get-Url -DatasetId $d.Id -Rows $MaxRows -ApiMode $UseApi.IsPresent
    $outFile = Join-Path -Path $OutputDir -ChildPath $d.File

    if ((Test-Path -LiteralPath $outFile) -and -not $Force) {
        Write-Host "[SKIP] $($d.Name): file exists -> $outFile" -ForegroundColor Yellow
        continue
    }

    Write-Host "[GET ] $($d.Name) from $url" -ForegroundColor Green
    try {
        Invoke-WebRequest -Uri $url -OutFile $outFile -Headers $headers -MaximumRedirection 5
        $sizeMB = [Math]::Round((Get-Item $outFile).Length / 1MB, 2)
        Write-Host "[OK  ] $($d.Name) saved to $outFile (${sizeMB}MB)" -ForegroundColor Green
    }
    catch {
        Write-Host "[FAIL] $($d.Name): $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

Write-Host "All requested downloads completed." -ForegroundColor Cyan
