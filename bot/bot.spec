# PyInstaller spec for bot.exe (combined lottery bot)
# Run: pyinstaller bot.spec   (from the bot/ directory)

import sys

block_cipher = None

# When running from bot/, run_bot.py is the entry point; el_gordo, euromillones, la_primitiva are in same dir
a = Analysis(
    ['run_bot.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        'el_gordo',
        'euromillones',
        'la_primitiva',
        'selenium',
        'selenium.webdriver',
        'selenium.webdriver.chrome',
        'selenium.webdriver.chrome.options',
        'selenium.webdriver.chrome.service',
        'selenium.webdriver.chrome.webdriver',
        'selenium.webdriver.common.by',
        'selenium.webdriver.support',
        'selenium.webdriver.support.expected_conditions',
        'selenium.webdriver.support.ui',
        'requests',
        'dotenv',
        'webdriver_manager',
        'webdriver_manager.chrome',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='bot',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
