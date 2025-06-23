# Export library metadata
$path_lib = "D:\Video Game Data Projects\data\raw\playnite\library_extracts\playnite_library.csv"
$PlayniteApi.Database.Games | Sort-Object -Property Name | Select Name, ID, @{Name='Categories'; Expr={($_.Categories | Select-Object -ExpandProperty "Name") -Join ', '}}, Hidden, CompletionStatus, ReleaseDate | ConvertTo-Csv | Out-File $path_lib -Encoding utf8