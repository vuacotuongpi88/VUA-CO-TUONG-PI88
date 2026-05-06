$srcDir = "images\avatar_frames"
$outDir = "images\avatar_frames_clean"

New-Item -ItemType Directory -Force -Path $outDir | Out-Null

Add-Type -AssemblyName System.Drawing

$files = @(
    "none.png",
    "bronze-crown.png",
    "jade-flower.png",
    "dragon-frame.png",
    "phoenix-frame.png"
)

function Is-BgPixel($r, $g, $b, $a) {
    if ($a -lt 20) { return $true }

    $max = [Math]::Max($r, [Math]::Max($g, $b))
    $min = [Math]::Min($r, [Math]::Min($g, $b))
    $diff = $max - $min

    # Xóa nền trắng / xám / caro giả trong ảnh
    if ($min -ge 215 -and $diff -le 45) { return $true }
    if ($r -ge 235 -and $g -ge 235 -and $b -ge 235) { return $true }

    return $false
}

foreach ($file in $files) {
    $path = Join-Path $srcDir $file

    if (!(Test-Path $path)) {
        Write-Host "THIEU FILE: $path" -ForegroundColor Red
        continue
    }

    Write-Host "DANG XU LY: $file" -ForegroundColor Yellow

    $imgRaw = [System.Drawing.Image]::FromFile($path)
    $img = New-Object System.Drawing.Bitmap($imgRaw)
    $imgRaw.Dispose()

    $newImg = New-Object System.Drawing.Bitmap($img.Width, $img.Height)

    for ($y = 0; $y -lt $img.Height; $y++) {
        for ($x = 0; $x -lt $img.Width; $x++) {
            $c = $img.GetPixel($x, $y)

            if (Is-BgPixel $c.R $c.G $c.B $c.A) {
                $transparent = [System.Drawing.Color]::FromArgb(0, $c.R, $c.G, $c.B)
                $newImg.SetPixel($x, $y, $transparent)
            } else {
                $newImg.SetPixel($x, $y, $c)
            }
        }
    }

    $outPath = Join-Path $outDir $file

    if (Test-Path $outPath) {
        Remove-Item $outPath -Force
    }

    $newImg.Save($outPath, [System.Drawing.Imaging.ImageFormat]::Png)

    $img.Dispose()
    $newImg.Dispose()

    Write-Host "DA XOA NEN: $outPath" -ForegroundColor Green
}

Write-Host "XONG. Anh sach nam trong: $outDir" -ForegroundColor Cyan