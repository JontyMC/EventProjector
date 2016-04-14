param(
	[string[]] $task = "default"
)

Push-Location .\src\
.nuget\NuGet.exe update -self
.nuget\NuGet.exe restore -MSBuildVersion 14
Pop-Location

Push-Location .\build
Import-Module -Name ..\src\packages\psake*\tools\psake.psm1
Invoke-psake -buildFile .\default.ps1 -taskList $task
Pop-Location

if ($psake.build_success -eq $false) { exit 1 } else { exit 0 }