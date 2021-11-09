@echo off
cd redis-db
echo/
echo Starting Redis Server...
echo Running storage engine with configured settings!
echo Note: do NOT close this window to keep running this service.
echo/
redis-server.exe db.conf
exit /b 0