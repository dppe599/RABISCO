@echo off
cd /d "%~dp0"
title RABISCO ONLINE
where node >nul 2>nul
if errorlevel 1 (
  echo Node.js nao foi encontrado. Instale Node.js 18 ou superior primeiro.
  echo https://nodejs.org/
  pause
  exit /b 1
)
echo.
echo Abrindo RABISCO ONLINE em http://localhost:3000
 echo Para amigos na mesma rede, use o IPv4 deste computador + :3000.
echo Para encerrar, pressione Ctrl+C.
echo.
call npm start
pause
