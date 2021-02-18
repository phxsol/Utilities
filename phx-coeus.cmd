@IF EXIST "%~dp0\node.exe" (
  "%~dp0\node.exe"  "bin\phx-coeus.js" %*
) ELSE (
  @SETLOCAL
  @SET PATHEXT=%PATHEXT:;.JS;=;%
  node  "bin\phx-coeus.js" %*
)