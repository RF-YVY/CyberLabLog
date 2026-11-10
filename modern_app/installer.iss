#ifndef AppVersion
  #error AppVersion must be supplied by build_windows.ps1
#endif

[Setup]
AppId={{CFD32DC6-4DD0-4D25-AD63-413C9F6E5CAE}
AppName=CyberLab Case Tracker
AppVersion={#AppVersion}
AppPublisher=RF-YVY
AppPublisherURL=https://github.com/RF-YVY/CyberLabLog
AppSupportURL=https://github.com/RF-YVY/CyberLabLog/issues
DefaultDirName={localappdata}\Programs\CyberLab Case Tracker
DefaultGroupName=CyberLab Case Tracker
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
OutputDir=..\dist
OutputBaseFilename=CyberLab-Case-Tracker-Setup-v{#AppVersion}
SetupIconFile=..\digital.ico
UninstallDisplayIcon={app}\CyberLab Case Tracker.exe
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
CloseApplications=yes
RestartApplications=no

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; Flags: unchecked

[Files]
Source: "..\dist\CyberLab Case Tracker.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\CyberLab Case Tracker"; Filename: "{app}\CyberLab Case Tracker.exe"
Name: "{autodesktop}\CyberLab Case Tracker"; Filename: "{app}\CyberLab Case Tracker.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\CyberLab Case Tracker.exe"; Description: "Open CyberLab Case Tracker"; Flags: nowait postinstall skipifsilent

; Case databases and app_data are created by the app, not installed or removed here.
