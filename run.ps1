# run_infero.ps1
# Run from inside the llm-inference-server-main folder, in your `infero` conda env.
# Starts Redis (if not already running), then scheduler/worker/API in separate
# windows, waits for health check, then fires a test request.

$ErrorActionPreference = "Stop"

Write-Host "=== 1. Checking Redis ===" -ForegroundColor Cyan
$redisRunning = docker ps --filter "ancestor=redis:7" --filter "status=running" -q
if (-not $redisRunning) {
    Write-Host "Starting Redis container..." -ForegroundColor Yellow
    docker run -d -p 6379:6379 redis:7 | Out-Null
    Start-Sleep -Seconds 2
} else {
    Write-Host "Redis already running." -ForegroundColor Green
}

Write-Host "`n=== 2. Starting batch scheduler ===" -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate infero; python -m server.batch_scheduler"

Write-Host "=== 3. Starting worker (loads model, first run downloads ~2.2GB) ===" -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate infero; python -m worker.worker"

Write-Host "=== 4. Starting API server ===" -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate infero; uvicorn server.api:app --reload --port 8000"

Write-Host "`n=== 5. Waiting for API to become healthy ===" -ForegroundColor Cyan
$healthy = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:8000/health" -Method Get -TimeoutSec 2
        if ($resp.status -eq "ok") {
            $healthy = $true
            break
        }
    } catch {
        Start-Sleep -Seconds 2
    }
}

if (-not $healthy) {
    Write-Host "API did not become healthy in time. Check the opened windows for errors." -ForegroundColor Red
    exit 1
}

Write-Host "API is healthy." -ForegroundColor Green

Write-Host "`n=== 6. Sending a test inference request ===" -ForegroundColor Cyan
Write-Host "(worker is likely still loading the model on first run - this may take a bit)" -ForegroundColor Yellow

$body = '{"prompt": "What is the capital of France?", "max_tokens": 30}'
try {
    $result = Invoke-RestMethod -Uri "http://localhost:8000/infer" -Method Post -ContentType "application/json" -Body $body -TimeoutSec 60
    Write-Host "`n--- Result ---" -ForegroundColor Green
    $result | Format-List
} catch {
    Write-Host "Request failed or timed out - check the worker window, model may still be loading." -ForegroundColor Red
    Write-Host $_
}

Write-Host "`n=== Done ===" -ForegroundColor Cyan
Write-Host "Three windows are open: scheduler, worker, API server (uvicorn --reload)."
Write-Host "Metrics: http://localhost:8000/metrics"
Write-Host "Close those windows manually to stop everything, or Ctrl+C in each."