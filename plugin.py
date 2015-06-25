# -*- coding: utf-8 -*-

"""
/***************************************************************************
Name                  : DB Manager
Description          : Database manager plugin for QGIS (ogr)
Date                    : June 25, 2015
copyright            : (C) 2015 by Cédric Christen
email                   : cch@sourcepole.ch

 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

# this will disable the dbplugin if the connector raise an ImportError
from .connector import GeopackageDBConnector

from PyQt4.QtCore import Qt,  QSettings
from PyQt4.QtGui import QIcon, QApplication, QAction
from qgis.gui import QgsMessageBar

from ..plugin import DBPlugin, Database, Table, VectorTable, RasterTable, TableField, TableIndex, TableTrigger, \
    InvalidDataException
from ...db_plugins import createDbPlugin

try:
    from . import resources_rc
except ImportError:
    pass

def classFactory():
    return GeopackageDBPlugin


class GeopackageDBPlugin(DBPlugin):
    @classmethod
    def icon(self):
        return QIcon(":/db_manager/geopackage/icon")

    @classmethod
    def typeName(self):
        return 'ogr'

    @classmethod
    def typeNameString(self):
        return 'Geopackage'

    @classmethod
    def providerName(self):
        return 'ogr'

    @classmethod
    def connectionSettingsKey(self):
        return '/Geopackage/connections'
        
    @classmethod
    def connectionSettings(self):
        return "gpkgpath"

    def databasesFactory(self, connection, uri):
        return SLDatabase(connection, uri)

    def connect(self, parent=None):
        self.conn_name = self.connectionName()
        settings = QSettings()
        settings.beginGroup("/%s/%s" % (self.connectionSettingsKey(), self.conn_name))

        if not settings.contains("gpkgpath"):  # non-existent entry?
            raise InvalidDataException(u'there is no defined database connection "%s".' % self.conn_name)

        database = settings.value("gpkgpath")

        import qgis.core
        uri = qgis.core.QgsDataSourceURI()
        uri.setDatabase(database)
        return self.connectToUri(uri)


class SLDatabase(Database):
    def __init__(self, connection, uri):
        Database.__init__(self, connection, uri)

    def connectorsFactory(self, uri):
        return GeopackageDBConnector(uri)


    def dataTablesFactory(self, row, db, schema=None):
        return SLTable(row, db, schema)

    def vectorTablesFactory(self, row, db, schema=None):
        return SLVectorTable(row, db, schema)

    def rasterTablesFactory(self, row, db, schema=None):
        return SLRasterTable(row, db, schema)


    def info(self):
        from .info_model import SLDatabaseInfo

        return SLDatabaseInfo(self)

    def sqlResultModel(self, sql, parent):
        from .data_model import SLSqlResultModel

        return SLSqlResultModel(self, sql, parent)

    def registerDatabaseActions(self, mainWindow):
        action = QAction(self.tr("Run &Vacuum"), self)
        mainWindow.registerAction(action, self.tr("&Database"), self.runVacuumActionSlot)

        Database.registerDatabaseActions(self, mainWindow)

    def runVacuumActionSlot(self, item, action, parent):
        QApplication.restoreOverrideCursor()
        try:
            if not isinstance(item, (DBPlugin, Table)) or item.database() is None:
                parent.infoBar.pushMessage(self.tr("No database selected or you are not connected to it."),
                                           QgsMessageBar.INFO, parent.iface.messageTimeout())
                return
        finally:
            QApplication.setOverrideCursor(Qt.WaitCursor)

        self.runVacuum()

    def runVacuum(self):
        self.database().aboutToChange()
        self.database().connector.runVacuum()
        self.database().refresh()


    def runAction(self, action):
        action = unicode(action)

        if action.startswith("vacuum/"):
            if action == "vacuum/run":
                self.runVacuum()
                return True

        return Database.runAction(self, action)

    def uniqueIdFunction(self):
        return None

    def explicitSpatialIndex( self ):
        return True

    def spatialIndexClause( self, src_table, src_column, dest_table, dest_column ):
        return """"%s".ROWID IN (\nSELECT ROWID FROM SpatialIndex WHERE f_table_name='%s' AND search_frame="%s"."%s") """ % (src_table,src_table,dest_table, dest_column)

class SLTable(Table):
    def __init__(self, row, db, schema=None):
        Table.__init__(self, db, None)
        self.name, self.isView = row
        self.name = self.name.GetName()


    def tableFieldsFactory(self, row, table):
        return SLTableField(row, table)

    def tableIndexesFactory(self, row, table):
        return SLTableIndex(row, table)

    def tableTriggersFactory(self, row, table):
        return SLTableTrigger(row, table)


    def tableDataModel(self, parent):
        from .data_model import SLTableDataModel

        return SLTableDataModel(self, parent)


class SLVectorTable(SLTable, VectorTable):
    def __init__(self, row, db, schema=None):
        SLTable.__init__(self, row[:-5], db, schema)
        VectorTable.__init__(self, db, schema)
        # Geopackage does case-insensitive checks for table names, but the
        # SL provider didn't do the same in QGis < 1.9, so self.geomTableName
        # stores the table name like stored in the geometry_columns table
        self.geomTableName, self.geomColumn, self.geomType, self.geomDim, self.srid = row[-5:]

    def uri(self):
        uri = self.database().uri()
        uri.setDataSource('', self.geomTableName, self.geomColumn)
        return uri

    def hasSpatialIndex(self, geom_column=None):
        geom_column = geom_column if geom_column is not None else self.geomColumn
        return self.database().connector.hasSpatialIndex((self.schemaName(), self.name), geom_column)

    def createSpatialIndex(self, geom_column=None):
        self.aboutToChange()
        ret = VectorTable.createSpatialIndex(self, geom_column)
        if ret is not False:
            self.database().refresh()
        return ret

    def deleteSpatialIndex(self, geom_column=None):
        self.aboutToChange()
        ret = VectorTable.deleteSpatialIndex(self, geom_column)
        if ret is not False:
            self.database().refresh()
        return ret

    def refreshTableEstimatedExtent(self):
        return


    def runAction(self, action):
        if SLTable.runAction(self, action):
            return True
        return VectorTable.runAction(self, action)


class SLRasterTable(SLTable, RasterTable):
    def __init__(self, row, db, schema=None):
        SLTable.__init__(self, row[:-3], db, schema)
        RasterTable.__init__(self, db, schema)
        self.prefixName, self.geomColumn, self.srid = row[-3:]
        self.geomType = 'RASTER'

        # def info(self):
        #from .info_model import SLRasterTableInfo
        #return SLRasterTableInfo(self)

    def gdalUri(self):
        uri = self.database().uri()
        gdalUri = u'RASTERLITE:%s,table=%s' % (uri.database(), self.prefixName)
        return gdalUri

    def mimeUri(self):
        uri = "raster:gdal:%s:%s" % (self.name, self.gdalUri())
        return uri

    def toMapLayer(self):
        from qgis.core import QgsRasterLayer, QgsContrastEnhancement

        rl = QgsRasterLayer(self.gdalUri(), self.name)
        if rl.isValid():
            rl.setContrastEnhancement(QgsContrastEnhancement.StretchToMinimumMaximum)
        return rl


class SLTableField(TableField):
    def __init__(self, row, table):
        TableField.__init__(self, table)
        self.num, self.name, self.dataType, self.notNull, self.default, self.primaryKey = row
        self.hasDefault = self.default


class SLTableIndex(TableIndex):
    def __init__(self, row, table):
        TableIndex.__init__(self, table)
        self.num, self.name, self.isUnique, self.columns = row


class SLTableTrigger(TableTrigger):
    def __init__(self, row, table):
        TableTrigger.__init__(self, table)
        self.name, self.function = row
