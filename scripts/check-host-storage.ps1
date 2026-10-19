& {
    $ErrorActionPreference = 'Stop'
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    $results = @()
    $entries = @(Get-ChildItem -LiteralPath 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss')
    foreach ($key in $entries) {
        $entry = Get-ItemProperty -LiteralPath $key.PSPath
        $base = [string]$entry.BasePath
        if ($base.StartsWith('\\?\') -or $base.StartsWith('\??\')) {
            $base = $base.Substring(4)
        }
        $row = [ordered]@{
            distribution = [string]$entry.DistributionName
            wsl_version = [int]$entry.Version
            base_path = $base
            observed_at = [DateTime]::UtcNow.ToString('o')
            required_free_bytes = 15GB
            gate_passed = $false
        }
        try {
            $vhdName = 'ext4.vhdx'
            if ($entry.PSObject.Properties.Name -contains 'VhdFileName' -and $entry.VhdFileName) {
                $vhdName = [string]$entry.VhdFileName
            }
            $vhdPath = Join-Path -Path $base -ChildPath $vhdName
            $drive = [System.IO.DriveInfo]::new([System.IO.Path]::GetPathRoot($vhdPath))
            $row.vhdx_path = $vhdPath
            $row.vhdx_exists = Test-Path -LiteralPath $vhdPath -PathType Leaf
            $row.host_volume = $drive.Name
            $row.available_free_bytes = $drive.AvailableFreeSpace
            $row.gate_passed = ($row.wsl_version -eq 2 -and $row.vhdx_exists -and $row.available_free_bytes -ge $row.required_free_bytes)
        } catch {
            $row.error = $_.Exception.GetType().Name
        }
        $results += $row
    }
    ConvertTo-Json -InputObject @($results) -Depth 5
}
