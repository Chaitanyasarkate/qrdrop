# QR-Drop: Windows Local Wi-Fi Phone-to-PC Bridge
# Automatically detects the active Windows wireless/LAN IPv4 address,
# updates PUBLIC_BASE_URL in .env, spins up the Docker stack, and prints mobile connection details.

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "         QR-Drop: Windows Local Wi-Fi Phone-to-PC Bridge         " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Detect active LAN IPv4 address (Prioritize Wi-Fi and default route interface)
$detectedIP = $null

try {
    # Method A: Default internet/LAN route interface
    $defaultRoute = Get-NetRoute -DestinationPrefix "0.0.0.0/0" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($defaultRoute) {
        $ipEntry = Get-NetIPAddress -InterfaceIndex $defaultRoute.InterfaceIndex -AddressFamily IPv4 -ErrorAction SilentlyContinue |
                   Where-Object { $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.254.*" } |
                   Select-Object -First 1
        if ($ipEntry) {
            $detectedIP = $ipEntry.IPAddress
        }
    }
} catch {}

# Method B: Search for Wi-Fi or active Ethernet adapters
if (-not $detectedIP) {
    try {
        $adapterIP = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
                     Where-Object { 
                         ($_.IPAddress -like "192.168.*" -or $_.IPAddress -like "10.*") -and
                         $_.InterfaceAlias -notlike "*vEthernet*" -and
                         $_.InterfaceAlias -notlike "*Loopback*"
                     } |
                     Select-Object -First 1
        if ($adapterIP) {
            $detectedIP = $adapterIP.IPAddress
        }
    } catch {}
}

if (-not $detectedIP) {
    Write-Host "[!] Could not detect active Wi-Fi IPv4 address. Defaulting to 127.0.0.1" -ForegroundColor Yellow
    $detectedIP = "127.0.0.1"
} else {
    Write-Host "[+] Detected Active Windows LAN IPv4 : $detectedIP" -ForegroundColor Green
}

# 2. Read configured host port from .env
$envPath = Join-Path $PSScriptRoot ".env"
$appPort = "8000"
if (Test-Path $envPath) {
    $envLines = Get-Content $envPath
    foreach ($line in $envLines) {
        if ($line -match "^APP_PORT=(\d+)") {
            $appPort = $matches[1]
        }
    }
}

$lanBaseUrl = "http://${detectedIP}:${appPort}"
Write-Host "[+] Configuring Target LAN Base URL  : $lanBaseUrl" -ForegroundColor Cyan

# 3. Update PUBLIC_BASE_URL in .env
if (Test-Path $envPath) {
    $envContent = Get-Content $envPath -Raw
    if ($envContent -match "PUBLIC_BASE_URL=.*") {
        $updatedEnv = $envContent -replace "PUBLIC_BASE_URL=.*", "PUBLIC_BASE_URL=$lanBaseUrl"
    } else {
        $updatedEnv = $envContent + "`nPUBLIC_BASE_URL=$lanBaseUrl"
    }
    Set-Content -Path $envPath -Value $updatedEnv -NoNewline
    Write-Host "[+] Successfully synchronized .env with LAN address." -ForegroundColor Green
}

# 4. Spin up the container stack
Write-Host "`n[+] Starting Docker Compose stack with updated bridge settings..." -ForegroundColor Yellow
docker compose up -d --build

# 5. Output ready message & mobile quickstart
Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host "                  QR-Drop Stack is Live & Ready!                 " -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
Write-Host " PC Web Dashboard   : http://localhost:${appPort}" -ForegroundColor White
Write-Host " Mobile Wi-Fi URL   : $lanBaseUrl" -ForegroundColor White
Write-Host " Interactive Swagger: http://localhost:${appPort}/docs" -ForegroundColor White
Write-Host " Storage Health     : http://localhost:${appPort}/health" -ForegroundColor White
Write-Host "`n-----------------------------------------------------------------" -ForegroundColor Gray
Write-Host " Quick CLI Test (Upload from PC or LAN client):" -ForegroundColor Cyan
Write-Host "   curl.exe -T sample.txt $lanBaseUrl/sample.txt" -ForegroundColor Yellow
Write-Host " Scan generated QR codes directly from your mobile camera on Wi-Fi!" -ForegroundColor White
Write-Host "=================================================================`n" -ForegroundColor Green
