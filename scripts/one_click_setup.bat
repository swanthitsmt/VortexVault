@echo off
setlocal

if not exist .env (
  copy .env.example .env >nul
)

docker compose up -d --build

echo App: http://localhost:8000
echo Flower: http://localhost:5555
