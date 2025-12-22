$ErrorActionPreference = "Stop"

$jar = "target/event-monitoring-backend-0.0.1-SNAPSHOT.jar"
if (-not (Test-Path $jar)) {
  throw "Jar not found at $jar. Run mvn -DskipTests package first."
}

$env:SPRING_DATASOURCE_URL = "jdbc:mariadb://localhost:3307/eventsdb"
$env:SPRING_DATASOURCE_USERNAME = "appuser"
$env:SPRING_DATASOURCE_PASSWORD = "apppass"
$env:AGGREGATION_WARMUP_ENABLED = "false"

$logDir = "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$outLog = Join-Path $logDir "perf-test.out.log"
$errLog = Join-Path $logDir "perf-test.err.log"
if (Test-Path $outLog) {
  Remove-Item $outLog -Force
}
if (Test-Path $errLog) {
  Remove-Item $errLog -Force
}

$proc = Start-Process -FilePath "java" -ArgumentList @("-jar", $jar) -PassThru -NoNewWindow `
  -RedirectStandardOutput $outLog -RedirectStandardError $errLog

try {
  $day = (Get-Date -Format "yyyy-MM-dd")
  $baseUrl = "http://localhost:8080"
  $ready = $false
  for ($i = 0; $i -lt 60 -and -not $ready; $i++) {
    Start-Sleep -Seconds 2
    try {
      Invoke-RestMethod -Uri "$baseUrl/api/v1/days/$day/events/payments.in/success?page=0&size=1" -Method Get | Out-Null
      $ready = $true
    } catch {
    }
  }
  if (-not $ready) {
    throw "App did not become ready in time."
  }

  $results = [ordered]@{}
  function Invoke-Timed([string]$label, [string]$uri) {
    $ms = (Measure-Command { Invoke-RestMethod -Uri $uri -Method Get | Out-Null }).TotalMilliseconds
    $results[$label] = [Math]::Round($ms, 2)
  }

  $eventKey = "payments.in"
  $coldPrefix = "cold"
  $warmPrefix = "warm"

  Invoke-Timed "$coldPrefix.home" "$baseUrl/api/v1/days/$day/home"
  Invoke-Timed "$coldPrefix.summary" "$baseUrl/api/v1/days/$day/events/$eventKey/summary"
  Invoke-Timed "$coldPrefix.buckets60" "$baseUrl/api/v1/days/$day/events/$eventKey/buckets?intervalMinutes=60"
  Invoke-Timed "$coldPrefix.buckets15" "$baseUrl/api/v1/days/$day/events/$eventKey/buckets?intervalMinutes=15"
  Invoke-Timed "$coldPrefix.success" "$baseUrl/api/v1/days/$day/events/$eventKey/success?page=0&size=50"
  Invoke-Timed "$coldPrefix.failures" "$baseUrl/api/v1/days/$day/events/$eventKey/failures?page=0&size=50"

  Invoke-Timed "$warmPrefix.home" "$baseUrl/api/v1/days/$day/home"
  Invoke-Timed "$warmPrefix.summary" "$baseUrl/api/v1/days/$day/events/$eventKey/summary"
  Invoke-Timed "$warmPrefix.buckets60" "$baseUrl/api/v1/days/$day/events/$eventKey/buckets?intervalMinutes=60"
  Invoke-Timed "$warmPrefix.buckets15" "$baseUrl/api/v1/days/$day/events/$eventKey/buckets?intervalMinutes=15"

  Start-Sleep -Seconds 2

  $cacheChecks = [ordered]@{}
  $logFiles = @($outLog, $errLog) | Where-Object { Test-Path $_ }
  $cacheChecks["computed.home"] = (Select-String -Path $logFiles -Pattern "Computed home aggregation").Count
  $cacheChecks["computed.summary"] = (Select-String -Path $logFiles -Pattern "Computed event summary").Count
  $cacheChecks["computed.buckets"] = (Select-String -Path $logFiles -Pattern "Computed buckets").Count

  $report = [ordered]@{
    day = $day
    timingsMs = $results
    cacheComputeCounts = $cacheChecks
  }
  $report | ConvertTo-Json -Depth 5
} finally {
  Stop-Process -Id $proc.Id -Force
}
