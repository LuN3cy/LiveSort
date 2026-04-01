@echo off
setlocal EnableExtensions
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."

set "REPO_URL=https://github.com/LuN3cy/LiveSort.git"
set "COMMIT_MSG=%~1"
if "%COMMIT_MSG%"=="" set "COMMIT_MSG=chore: update LiveSort"

where git >nul 2>nul
if errorlevel 1 (
    echo [LIVESORT_DEPLOY] Git is not installed or not available in PATH.
    pause
    exit /b 1
)

if not exist ".git" if exist "LiveSortApp\.git" (
    echo [LIVESORT_DEPLOY] Found LiveSortApp\.git but root .git is missing.
    echo [LIVESORT_DEPLOY] Please migrate repository root manually, then run deploy again.
    pause
    exit /b 1
)

if not exist ".git" (
    echo [LIVESORT_DEPLOY] Initializing repository...
    git init
    if errorlevel 1 (
        echo [LIVESORT_DEPLOY] Failed to initialize git repository.
        pause
        exit /b 1
    )
)

git branch -M main >nul 2>nul

for /f "delims=" %%i in ('git config --get user.name 2^>nul') do set "GIT_USER_NAME=%%i"
if not defined GIT_USER_NAME git config user.name "LuN3cy"

for /f "delims=" %%i in ('git config --get user.email 2^>nul') do set "GIT_USER_EMAIL=%%i"
if not defined GIT_USER_EMAIL git config user.email "noreply@lun3cy.top"

git remote get-url origin >nul 2>nul
if errorlevel 1 (
    echo [LIVESORT_DEPLOY] Adding remote origin...
    git remote add origin "%REPO_URL%"
) else (
    echo [LIVESORT_DEPLOY] Updating remote origin...
    git remote set-url origin "%REPO_URL%"
)

echo [LIVESORT_DEPLOY] Staging files...
git add LiveSortApp
git add .github/workflows/deploy-pages.yml
if errorlevel 1 (
    echo [LIVESORT_DEPLOY] Failed to stage files.
    pause
    exit /b 1
)

git diff --cached --quiet
if errorlevel 1 (
    echo [LIVESORT_DEPLOY] Creating commit...
    git commit -m "%COMMIT_MSG%"
    if errorlevel 1 (
        echo [LIVESORT_DEPLOY] Commit failed.
        pause
        exit /b 1
    )
) else (
    echo [LIVESORT_DEPLOY] No staged changes to commit.
)

echo [LIVESORT_DEPLOY] Fetching remote state...
git fetch origin main >nul 2>nul

git ls-remote --exit-code --heads origin main >nul 2>nul
if not errorlevel 1 (
    echo [LIVESORT_DEPLOY] Rebasing local commits onto origin/main...
    git pull --rebase origin main
    if errorlevel 1 (
        echo [LIVESORT_DEPLOY] Rebase failed. Resolve conflicts, then run deploy again.
        git rebase --abort >nul 2>nul
        pause
        exit /b 1
    )
)

echo [LIVESORT_DEPLOY] Pushing to GitHub...
git push -u origin main
if errorlevel 1 (
    echo [LIVESORT_DEPLOY] Push failed. Remote may have new commits or branch protection is enabled.
    echo [LIVESORT_DEPLOY] Please run: git pull --rebase origin main
    pause
    exit /b 1
)

echo [LIVESORT_DEPLOY] Push complete.
echo [LIVESORT_DEPLOY] GitHub Pages URL: https://lun3cy.github.io/LiveSort/
pause
