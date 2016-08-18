del *.nupkg 
nuGet pack NBitcoin.Indexer.csproj -Build -Properties Configuration=Release -Properties GitLink=True -includereferencedprojects
forfiles /m *.nupkg /c "cmd /c NuGet.exe push @FILE -source https://api.nuget.org/v3/index.json"
(((dir *.nupkg).Name) -match "[0-9]+?\.[0-9]+?\.[0-9]+?\.[0-9]+")
$ver = $Matches.Item(0)
git tag -a "v$ver" -m "$ver"
git push --tags

msbuild.exe ../Build/Build.csproj /p:Configuration=Release
msbuild.exe ../NBitcoin.Indexer.Console/NBitcoin.Indexer.Console.csproj /p:Configuration=Release
xcopy /Y ..\NBitcoin.Indexer.Console\EmptyLocalSettings.config ..\NBitcoin.Indexer.Console\bin\Release\LocalSettings.config
msbuild.exe ../Build/DeployClient.proj
xcopy /Y ..\NBitcoin.Indexer.Console\LocalSettings.config ..\NBitcoin.Indexer.Console\bin\Release\LocalSettings.config