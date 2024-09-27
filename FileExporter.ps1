<# 
    Copyright ©2000-2023 NL Technology, LLC
	All Rights Reserved

	This code and information is provided "as is" without warranty of any
	kind, either expressed or implied, including but not limited to the
	implied warranties of merchantability and/or fitness for a particular
	purpose. 

	Its purpose is to provide a CSV file that contain the scan results 
	of the complete or a portion of the Xen Archive filesystem
#>

<#
Options

-filter (default "")  include everything
filesystem will include these files starting at the path provided (case sensitive)
wildcards * and ? are supported  See POWERSHELL get-childitem -filter documentation

-path (default "/")
starting location where the file search will begin  (case sensitive)
DO NOT INCLUDE A DRIVE LTR

-r
starting at the specified path, recursively search filesystem

-o  (default  c:/temp/file-exporter_<date>.csv )
specify the output file. NOTE: include a full path, a drive letter and extension
this can also be a UNC path as well
note the file will be deleted if it exists

NOTE CASE SENSITIVITY
to find a specific file set -path to the folder path and -filter to its name
to get all the files in a folder set -path to the folder's path, optionally: include -r or -filter

#>

#Params    
Param (
	[Alias("filter")][string]$cmd="",
	[Alias("p")][string]$path="\",
	[Alias("r")][switch]$recurse,
    [Alias("o")][string]$outputFile="c:\temp\",
    [Alias("x")][switch]$ignoreExceptions
    )

#**************** begin function

function getassets
{
    $filename = $_.FullName
	$filesize = $_.Length
	$creation = $_.CreationTime
	$lastaccessed = $_.LastWriteTime
	$global:filesfound +=1
	
	if (Test-Path $filename -pathType container)
	{
		$filetype="Folder"
		$flushed=""
		$volumeidentity=""
		$volumeonline=""
#		write-host ("folder: $filename")
	}
	else
	{ 
	Try {
		$filetype="File"
		$file = $system.FindFile($filename)
		$volumes = $file.Volumes
#		write-host ("file: $filename")
	
#		see if the file has been flushed from filesystem
		if ($_.Attributes -band [System.IO.FileAttributes]::Offline) { $flushed = $TRUE  } else { $flushed = $FALSE }
	
#		check if a volume has been assigned, if so get the container names
		if ($volumes.Count -eq 0) {
#			since no volume name there are no container names assigned
			$volumeonline = $FALSE
			$volumeidentity = ""
		} else
		{
#			get the names of the containers associated with the volume
			$volumeidentity = ""
			foreach($cartridge in $volumes.Cartridges) {
				if ($volumeidentity.Length) { $volumeidentity += "," }
					$volumeidentity += $cartridge.Identity
			}
		
#			we consider a replicated volume to be OnLine if at least one container is OnLine
			$volumeonline = $FALSE
			foreach ($volume in $file.volumes) {
				if ($volume.OnLine) { $volumeonline = $TRUE }
			}
		}
      }
	  catch {
		$message = $_.Exception.Message
		Write-Host ("[$message] at file: $filename")
	  }
	}
    
#	write the results for this file to the output stream
	$outWriter.WriteLine("$filename|$filetype|$filesize|$creation|$lastaccessed|$flushed|$volumeidentity|$volumeonline")
}

#**************** end function



#Script copyright and version to be written to console
$scriptCopyrightInformation = "Copyright 2000-2023 NL Technology LLC"
$scriptVersionInformation = "File-Exporter V1.0"
Write-Host "`n$scriptCopyrightInformation" -ForegroundColor Cyan
Write-Host "$scriptVersionInformation" -ForegroundColor Cyan

#Script requires PowerShell version 5, quit if requirement not met
if($($PSVersionTable.PSVersion).Major -lt 5){
    
    Write-Warning "PowerShell version 5 or greater is required to run this script, quitting..."
    break;
}

#Create a XenData.System COM object
$system = New-Object -ComObject XenData.System
$driveltr = $system.Root(0).UserPath
$driveltr = [regex]::Replace($system.Root(0).UserPath, '^\\\\\?\\','')
$root =  $path

$timer = [System.Diagnostics.Stopwatch]::StartNew()
$date = Get-Date -UFormat "%Y%m%d_%H%M%S"

Try {

$global:filesfound=0

if ($outputFile -eq "c:\temp\") { $outputFile += "File-Exporter_$date.csv" }
#$errorFile = $(Split-Path $outputFile) + "\fragoutNEW_$date" +"_ErrorLog.txt"

#Create output files
New-Item $outputFile -Force -ItemType File | Out-Null  
$outWriter = new-object System.IO.StreamWriter $outputFile


#****************************Main Body******************************

#$outWriter.WriteLine("sep=|")
#$outWriter.WriteLine("Date= $date")
#$outWriter.WriteLine("Path= $root")
#$outWriter.WriteLine("Filter= $cmd")
#$outWriter.WriteLine("Output file= $outputFile")
$outWriter.WriteLine("File-Path|File-Type|File-Size|Creation|Last-Accessed|File-Flushed|Container|Container-Online")

write-host "`nChecking for files in: $root" -ForegroundColor Cyan
write-host "Filter ""$cmd""" -ForegroundColor Cyan

if ($recurse)	{ Get-ChildItem -ErrorAction Ignore -Path $root -recurse -filter "$cmd" | ForEach-Object { getassets }} else 
				{ Get-ChildItem -ErrorAction Ignore -Path $root -filter "$cmd" | ForEach-Object { getassets }}
}
catch {
}
finally{
<#******************************************************************

 Prepare ending summary

 Write script version information footer to output files
 $outWriter.WriteLine("`n$scriptVersionInformation")
 $outWriter.WriteLine($scriptCopyrightInformation)
 
#>

$timer.Stop()
write-host ("Total Files Found: $filesfound") -ForegroundColor Cyan
Write-Host "Runtime:  $($timer.Elapsed)`n" -ForegroundColor Cyan
$timer.Reset()

$outWriter.Close()

#write-host
#$driveltr = Read-Host -Prompt '>>> Press Enter to close this window.'
}