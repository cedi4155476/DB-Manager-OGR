
FILE(GLOB PY_FILES *.py)
FILE(GLOB ICON_FILES icons/*.png)

PYQT4_ADD_RESOURCES(PYRC_FILES resources.qrc)

PLUGIN_INSTALL(db_manager db_plugins/geopackage ${PY_FILES} ${PYRC_FILES})
PLUGIN_INSTALL(db_manager db_plugins/geopackage/icons ${ICON_FILES})

