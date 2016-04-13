properties {
    $majorVersion = "0.0"
    $buildNumber = if ( "$env:BUILD_NUMBER".length -gt 0 ) { "$env:BUILD_NUMBER" } else { "0" }
    $version = "$majorVersion.$buildNumber"
    $baseDir = Resolve-Path ..
    $buildDir = "$baseDir\build"
    $binDir = "$baseDir\bin"
    $artifactsDir = "$baseDir\artifacts\"
    $srcDir = "$baseDir\src"
    $buildType = if ($buildType -eq $null) { "debug" } else { $buildType }
    $slnFile = "$srcDir\EventProjector.sln"
}

task default -depends Compile, Package

Task Clean {
    if (Test-Path $artifactsDir) 
    {   
        rd $artifactsDir -rec -force | out-null
    }

    mkdir $artifactsDir | out-null
    Exec { msbuild "$slnFile" /t:Clean /p:Configuration=$buildType /v:quiet /p:OutDir=$binDir } 
}

Task Compile -Depends Clean { 
    Exec { msbuild "$slnFile" /t:Build /p:Configuration=$buildType /v:quiet /p:OutDir=$binDir }  
}

Task Package {
    $nuget = "$srcDir\.nuget\nuget.exe"
    $nuspec = "$buildDir\EventProjector.nuspec"
    Exec { & $nuget pack $nuspec -Version $version -OutputDirectory $artifactsDir -NoPackageAnalysis }
}