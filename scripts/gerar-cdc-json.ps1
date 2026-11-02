# Gera connectors/cdc.json a partir do template, preenchendo ${VARIAVEL} com os valores do .env.
# Rodar na raiz do repositório:  .\scripts\gerar-cdc-json.ps1
# O cdc.json gerado contém a senha do banco e está no .gitignore.

Get-Content .env | ForEach-Object {
  if ($_ -match '^([^=]+)=(.*)$') { Set-Item "env:$($matches[1].Trim())" $matches[2].Trim() }
}
$t = Get-Content connectors\cdc.json.template -Raw
$o = [regex]::Replace($t, '\$\{(\w+)\}', { param($m) [Environment]::GetEnvironmentVariable($m.Groups[1].Value) })
Set-Content connectors\cdc.json $o -Encoding ascii
Write-Output "connectors\cdc.json gerado."
