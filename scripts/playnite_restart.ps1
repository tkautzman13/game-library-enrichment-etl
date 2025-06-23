# restart-playnite.ps1

# Kill existing Playnite fullscreen session (if running)
Get-Process "Playnite.FullscreenApp" -ErrorAction SilentlyContinue | Stop-Process -Force

# Wait a few seconds to ensure it's closed
Start-Sleep -Seconds 3

# Start Playnite in fullscreen mode
Start-Process "C:\Users\Tkaut\AppData\Local\Playnite\Playnite.FullscreenApp.exe"