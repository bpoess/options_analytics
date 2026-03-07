# -*- mode: python ; coding: utf-8 -*-


analysis_get_transactions = Analysis(
    ['src/options_analytics/get_transactions.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz_get_transactions = PYZ(analysis_get_transactions.pure)

exe_get_transactions = EXE(
    pyz_get_transactions,
    analysis_get_transactions.scripts,
    [],
    exclude_binaries=True,
    name='get_transactions',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

analysis_update_spreadsheet = Analysis(
    ['src/options_analytics/update_spreadsheet.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz_update_spreadsheet = PYZ(analysis_update_spreadsheet.pure)

exe_update_spreadsheet = EXE(
    pyz_update_spreadsheet,
    analysis_update_spreadsheet.scripts,
    [],
    exclude_binaries=True,
    name='update_spreadsheet',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

analysis_fetch_data = Analysis(
    ['src/options_analytics/fetch_data.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz_fetch_data = PYZ(analysis_fetch_data.pure)

exe_fetch_data = EXE(
    pyz_fetch_data,
    analysis_fetch_data.scripts,
    [],
    exclude_binaries=True,
    name='fetch_data',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

analysis_setup_config = Analysis(
    ['src/options_analytics/setup_config.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz_setup_config = PYZ(analysis_setup_config.pure)

exe_setup_config = EXE(
    pyz_setup_config,
    analysis_setup_config.scripts,
    [],
    exclude_binaries=True,
    name='options_analytics_setup_config',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe_get_transactions, analysis_get_transactions.binaries, analysis_get_transactions.datas,
    exe_update_spreadsheet, analysis_update_spreadsheet.binaries, analysis_update_spreadsheet.datas,
    exe_fetch_data, analysis_fetch_data.binaries, analysis_fetch_data.datas,
    exe_setup_config, analysis_setup_config.binaries, analysis_setup_config.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='options_analytics',
)
