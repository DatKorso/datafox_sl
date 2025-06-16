@echo off
chcp 65001 >nul
cls

echo ====================================================
echo              DataFox SL - Запуск приложения
echo ====================================================
echo.

:: Параметр для пропуска обновлений (для избежания бесконечного цикла)
if "%1"=="--skip-update" goto skip_update

:: Проверка файла пропуска обновлений на сегодня
if exist ".skip_update_today" (
    for /f %%i in (.skip_update_today) do set SKIP_DATE=%%i
    if "!SKIP_DATE!"=="%date%" (
        echo ⏭️  Проверка обновлений пропущена на сегодня
        goto skip_update
    ) else (
        del .skip_update_today >nul 2>&1
    )
)

:: Проверка автоматических обновлений
echo [0/6] Проверяю обновления...

:: Проверка наличия Git (тихо)
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ⚠️  Git не найден - автообновления отключены
    echo 💡 Для автообновлений установите Git: https://git-scm.com/download/win
    echo.
    goto skip_update
)

:: Проверка git репозитория
if exist ".git" (
    echo 🔍 Проверяю наличие обновлений...
    
    :: Быстрая проверка обновлений (timeout 10 секунд)
    git fetch origin >nul 2>&1
    if %errorlevel% equ 0 (
        for /f %%i in ('git rev-list HEAD...origin/main --count 2^>nul') do set UPDATE_COUNT=%%i
        if not "%UPDATE_COUNT%"=="0" (
            echo 📦 Найдено %UPDATE_COUNT% обновлений!
            echo.
            echo Обновить до последней версии? (y/n/s)
            echo   y - Да, обновить (рекомендуется)
            echo   n - Нет, запустить текущую версию
            echo   s - Пропустить проверку на сегодня
            echo.
            set /p choice="Ваш выбор: "
            
            if /i "!choice!"=="y" (
                echo.
                echo 🔄 Обновляю до последней версии...
                call :update_system
                if !errorlevel! equ 0 (
                    echo ✅ Обновление завершено! Перезапускаю приложение...
                    echo.
                    "%~f0" --skip-update
                    exit /b 0
                ) else (
                    echo ❌ Ошибка обновления. Запускаю текущую версию...
                    echo.
                )
            ) else if /i "!choice!"=="s" (
                echo 📅 Создаю файл пропуска на 24 часа...
                echo %date% > .skip_update_today
            )
        ) else (
            echo ✅ У вас последняя версия
        )
    ) else (
        echo ⚠️  Не удалось проверить обновления (нет интернета?)
    )
) else (
    echo ℹ️  Git репозиторий не инициализирован
)

echo.

:skip_update
:: Включаем отложенное расширение переменных
setlocal enabledelayedexpansion

:: Проверка наличия Python
echo [1/5] Проверяю наличие Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python не найден!
    echo.
    echo 📥 Скачайте Python с официального сайта:
    echo https://www.python.org/downloads/
    echo.
    echo Выберите версию Python 3.8 или новее
    echo ⚠️  ВАЖНО: Во время установки поставьте галочку "Add Python to PATH"
    echo.
    pause
    exit /b 1
) else (
    for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
    echo ✅ Python найден: %PYTHON_VERSION%
)

echo.
echo [2/5] Проверяю виртуальную среду...

:: Проверка и создание виртуальной среды
if not exist "venv" (
    echo 🔧 Создаю виртуальную среду...
    python -m venv venv
    if %errorlevel% neq 0 (
        echo ❌ Ошибка создания виртуальной среды!
        pause
        exit /b 1
    )
    echo ✅ Виртуальная среда создана
) else (
    echo ✅ Виртуальная среда уже существует
)

echo.
echo [3/5] Активирую виртуальную среду...
call venv\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo ❌ Ошибка активации виртуальной среды!
    pause
    exit /b 1
)
echo ✅ Виртуальная среда активирована

echo.
echo [4/5] Проверяю зависимости...

:: Проверка pip и обновление
python -m pip install --upgrade pip >nul 2>&1

:: Проверка наличия streamlit (основная зависимость)
python -c "import streamlit" >nul 2>&1
if %errorlevel% neq 0 (
    echo 📦 Устанавливаю зависимости...
    echo Это может занять несколько минут...
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo ❌ Ошибка установки зависимостей!
        echo Проверьте подключение к интернету и попробуйте снова
        pause
        exit /b 1
    )
    echo ✅ Зависимости установлены
) else (
    echo ✅ Зависимости уже установлены
)

echo.
echo [5/5] Запускаю приложение...
echo.
echo 🚀 DataFox SL запускается...
echo 🌐 Приложение откроется в браузере по адресу: http://localhost:8501
echo 🔄 Для остановки нажмите Ctrl+C в этом окне
echo.

:: Запуск приложения
streamlit run main_app.py --server.port=8501 --server.headless=false
if %errorlevel% neq 0 (
    echo.
    echo ❌ Ошибка запуска приложения!
    echo Возможные причины:
    echo - Порт 8501 уже занят
    echo - Проблемы с конфигурацией
    echo.
    pause
    exit /b 1
)

echo.
echo 👋 Приложение завершено
pause
exit /b 0

:: Функция быстрого обновления системы
:update_system
setlocal

:: Создание бэкапа важных файлов
set BACKUP_DIR=backup_temp_%random%
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"

if exist "config.json" copy "config.json" "%BACKUP_DIR%\" >nul 2>&1
if exist "data" xcopy "data" "%BACKUP_DIR%\data\" /E /I /Q >nul 2>&1

:: Сохранение и применение обновлений
git add . >nul 2>&1
git stash >nul 2>&1
git reset --hard origin/main >nul 2>&1

if %errorlevel% neq 0 (
    echo ❌ Ошибка применения обновлений
    exit /b 1
)

:: Восстановление пользовательских файлов
if exist "%BACKUP_DIR%\config.json" copy "%BACKUP_DIR%\config.json" "." >nul 2>&1
if exist "%BACKUP_DIR%\data" xcopy "%BACKUP_DIR%\data\*" "data\" /E /Y /Q >nul 2>&1

:: Очистка временных файлов
rmdir /s /q "%BACKUP_DIR%" >nul 2>&1

:: Обновление зависимостей если нужно
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
    pip install -r requirements.txt --upgrade >nul 2>&1
)

exit /b 0 