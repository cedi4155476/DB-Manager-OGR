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

from PyQt4.QtCore import QFile
from PyQt4.QtGui import QApplication

from ..connector import DBConnector
from ..plugin import ConnectionError, DbError, Table

from osgeo import ogr, gdal

def classFactory():
    return GeopackageDBConnector


class GeopackageDBConnector(DBConnector):
    def __init__(self, uri):
        DBConnector.__init__(self, uri)
        self.dbname = uri.database()
        self.connString = self._connectionInfo()
        
        if not QFile.exists(self.dbname):
            raise ConnectionError(QApplication.translate("DBManagerPlugin", '"{0}" not found').format(self.dbname))
        try:
            self.connection = ogr.Open(self.connString)

        except self.connection_error_types(), e:
            raise ConnectionError(e)

        self._checkSpatial()
        self._checkRaster()
#        self._checkGeometryColumnsTable()
#        self._checkRastersTable()

    def _connectionInfo(self):
        return unicode(self.dbname)

    def _checkSpatial(self):
        """ check if it's a valid geopackage db """
        self.has_spatial = self._checkGeometryColumnsTable()
        return self.has_spatial

    def _checkRaster(self):
        """ check if it's a rasterite db """
        self.has_raster = False  # self._checkRastersTable()
        return self.has_raster

    def _checkGeometryColumnsTable(self):
#        TODO: check geometry
#        try:
#            c = self._get_cursor()
#            self._execute(c, "SELECT CheckSpatialMetaData()")
#            v = c.fetchone()[0]
#            self.has_geometry_columns = v == 1 or v == 3
#            self.has_geopackage4 = v == 3
#        except Exception, e:
#            self.has_geometry_columns = False
#            self.has_geopackage4 = False

#        self.has_geometry_columns_access = self.has_geometry_columns
        self.has_geometry_columns = False
        self.has_geopackage4 = False
        return True

    def _checkRastersTable(self):
        
        return ret and ret[0]

    def getInfo(self):
#        c = self.connection
#        c = c.ExecuteSQL("SELECT sqlite_version()")
        return gdal.__version__

    def getSpatialInfo(self):
        """ returns tuple about geopackage support:
                - lib version
                - geos version
                - proj version
        """
        if not self.has_spatial:
            return

        c = self.connection
        try:
            c = c.ExecuteSQL("SELECT geopackage_version(), geos_version(), proj4_version()")
        except DbError:
            return

        return c

    def hasSpatialSupport(self):
        return self.has_spatial

    def hasRasterSupport(self):
        return self.has_raster

    def hasCustomQuerySupport(self):
        from qgis.core import QGis

        return QGis.QGIS_VERSION[0:3] >= "1.6"

    def hasTableColumnEditingSupport(self):
        return False

    def hasCreateSpatialViewSupport(self):
        return True

    def fieldTypes(self):
        return [
            "integer", "bigint", "smallint",  # integers
            "real", "double", "float", "numeric",  # floats
            "varchar", "varchar(255)", "character(20)", "text",  # strings
            "date", "datetime"  # date/time
        ]


    def getSchemas(self):
        return None

    def getTables(self, schema=None, add_sys_tables=False):
        """ get list of tables """
        tablenames = []
        items = []

        sys_tables = ["SpatialIndex", "geom_cols_ref_sys", "geometry_columns", "geometry_columns_auth",
                      "views_geometry_columns", "virts_geometry_columns", "spatial_ref_sys",
                      "sqlite_sequence",  # "tableprefix_metadata", "tableprefix_rasters",
                      "layer_params", "layer_statistics", "layer_sub_classes", "layer_table_layout",
                      "pattern_bitmaps", "symbol_bitmaps", "project_defs", "raster_pyramids",
                      "sqlite_stat1", "sqlite_stat2", "geopackage_history",
                      "geometry_columns_field_infos",
                      "geometry_columns_statistics", "geometry_columns_time",
                      "sql_statements_log","vector_layers", "vector_layers_auth", "vector_layers_field_infos", "vector_layers_statistics",
                      "views_geometry_columns_auth", "views_geometry_columns_field_infos", "views_geometry_columns_statistics",
                      "virts_geometry_columns_auth", "virts_geometry_columns_field_infos", "virts_geometry_columns_statistics"
                  ]

        try:
            vectors = self.getVectorTables(schema)
            for tbl in vectors:
                if not add_sys_tables and tbl[1] in sys_tables:
                    continue
                tablenames.append(tbl.GetName())
                items.append(tbl)
        except DbError:
            pass

        try:
            rasters = self.getRasterTables(schema)
            for tbl in rasters:
                if not add_sys_tables and tbl[1] in sys_tables:
                    continue
                tablenames.append(tbl[1])
                items.append(tbl)
        except DbError:
            pass

        c = self.connection

        if self.has_geometry_columns:
            # get the R*Tree tables
            sql = "SELECT f_table_name, f_geometry_column FROM geometry_columns WHERE spatial_index_enabled = 1"
            for idx_item in c.ExecuteSQL(sql):
                sys_tables.append('idx_%s_%s' % idx_item)
                sys_tables.append('idx_%s_%s_node' % idx_item)
                sys_tables.append('idx_%s_%s_parent' % idx_item)
                sys_tables.append('idx_%s_%s_rowid' % idx_item)

        sql = "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view')"


        for tbl in c:
            if tablenames.count(tbl[0]) <= 0:
                if not add_sys_tables and tbl[0] in sys_tables:
                    continue
                item = [tbl]
                item.insert(0, Table.TableType)
                items.append(item)

        for i, tbl in enumerate(items):
            tbl.insert(3, tbl[1] in sys_tables)

        return sorted(items, cmp=lambda x, y: cmp(x[1], y[1]))

    def getVectorTables(self, schema=None):
        """ get list of table with a geometry column
                it returns:
                        name (table name)
                        type = 'view' (is a view?)
                        geometry_column:
                                f_table_name (the table name in geometry_columns may be in a wrong case, use this to load the layer)
                                f_geometry_column
                                type
                                coord_dimension
                                srid
        """

        if not self.has_geometry_columns:
            return []

        c = self.connection

        if self.has_geopackage4:
            cols = """CASE geometry_type % 10
                                  WHEN 1 THEN 'POINT'
                                  WHEN 2 THEN 'LINESTRING'
                                  WHEN 3 THEN 'POLYGON'
                                  WHEN 4 THEN 'MULTIPOINT'
                                  WHEN 5 THEN 'MULTILINESTRING'
                                  WHEN 6 THEN 'MULTIPOLYGON'
                                  WHEN 7 THEN 'GEOMETRYCOLLECTION'
                                  END AS gtype,
                                  CASE geometry_type / 1000
                                  WHEN 0 THEN 'XY'
                                  WHEN 1 THEN 'XYZ'
                                  WHEN 2 THEN 'XYM'
                                  WHEN 3 THEN 'XYZM'
                                  ELSE NULL
                                  END AS coord_dimension"""
        else:
            cols = "g.type,g.coord_dimension"

        # get geometry info from geometry_columns if exists
        sql = """SELECT m.name, m.type = 'view', g.f_table_name, g.f_geometry_column, %s, g.srid
                                                FROM sqlite_master AS m JOIN geometry_columns AS g ON upper(m.name) = upper(g.f_table_name)
                                                WHERE m.type in ('table', 'view')
                                                ORDER BY m.name, g.f_geometry_column""" % cols

        c = c.ExecuteSQL(sql)

        items = []
        for tbl in c:
            item = list(tbl)
            item.insert(0, Table.VectorType)
            items.append(item)

        return items


    def getRasterTables(self, schema=None):
        """ get list of table with a geometry column
                it returns:
                        name (table name)
                        type = 'view' (is a view?)
                        geometry_column:
                                r.table_name (the prefix table name, use this to load the layer)
                                r.geometry_column
                                srid
        """

        if not self.has_geometry_columns:
            return []
        if not self.has_raster:
            return []

        c = self.connection

        # get geometry info from geometry_columns if exists
        sql = """SELECT r.table_name||'_rasters', m.type = 'view', r.table_name, r.geometry_column, g.srid
                                                FROM sqlite_master AS m JOIN geometry_columns AS g ON upper(m.name) = upper(g.f_table_name)
                                                JOIN layer_params AS r ON upper(REPLACE(m.name, '_metadata', '')) = upper(r.table_name)
                                                WHERE m.type in ('table', 'view') AND upper(m.name) = upper(r.table_name||'_metadata')
                                                ORDER BY r.table_name"""

        c = c.ExecuteSQL(sql)

        items = []
        for i, tbl in enumerate(c):
            item = list(tbl)
            item.insert(0, Table.RasterType)
            items.append(item)

        return items

    def getTableRowCount(self, table):
        c = self.connection
        sql = str("SELECT * FROM %s" % self.quoteId(table))
        ret = c.ExecuteSQL(sql)
        return ret.__len__() if ret is not None else None

    def getTableFields(self, table):
        """ return list of columns in table """
        conn = self.connection
        sql = str(table[1])
        c = conn.GetLayer(sql)
        
        if c:
            feat = c.GetFeature(1)
            count = feat.GetFieldCount()
            data = []
    
            data.append((0, c.GetFIDColumn(), 'Integer', None, None, None))
            data.append((1, c.GetGeometryColumn(), 'Geometry', None, None, None))
            for i in range(count):
                info = feat.GetFieldDefnRef(i)
                data.append((feat.GetFieldIndex(info.GetName()) + 2, info.GetName(), info.GetTypeName(), None, None, None))
            return data
        return None

    def getTableIndexes(self, table):
        """ get info about table's indexes """
#        c = self.connection
#        sql = str("PRAGMA index_list(%s)" % (str(table[1])))
#        indexes = c.ExecuteSQL(sql)
#
#        for i, idx in enumerate(indexes):
#            num, name, unique = idx
#            sql = str("PRAGMA index_info(%s)" % (self.quoteId(name)))
#            c = c.ExecuteSQL(sql)
#
#            idx = [num, name, unique]
#            cols = []
#            for seq, cid, cname in c:
#                cols.append(cid)
#            idx.append(cols)
#            indexes[i] = idx

        return []

    def getTableConstraints(self, table):
        return None

    def getTableTriggers(self, table):
        c = self.connection
        schema, tablename = self.getSchemaTableName(table)
        sql = str("SELECT name, sql FROM sqlite_master WHERE tbl_name = %s AND type = 'trigger'" % (
            self.quoteString(tablename)))
        c = c.ExecuteSQL(sql)
        return c

    def deleteTableTrigger(self, trigger, table=None):
        """ delete trigger """
        sql = str("DROP TRIGGER %s" % self.quoteId(trigger))
        self.connection.ExecuteSQL(sql)


    def getTableExtent(self, table, geom):
        """ find out table extent """
        schema, tablename = self.getSchemaTableName(table)
        c = self.connection

        if self.isRasterTable(table):
            tablename = tablename.replace('_rasters', '_metadata')
            geom = 'geometry'

        sql = str("""SELECT Min(MbrMinX(%(geom)s)), Min(MbrMinY(%(geom)s)), Max(MbrMaxX(%(geom)s)), Max(MbrMaxY(%(geom)s))
                                                FROM %(table)s """ % {'geom': self.quoteId(geom),
                                                                      'table': self.quoteId(tablename)})
        c = c.ExecuteSQL(sql)
        return c

    def getViewDefinition(self, view):
        """ returns definition of the view """
#        schema, tablename = self.getSchemaTableName(view)
#        sql = str("SELECT sql FROM sqlite_master WHERE type = 'view' AND name = %s" % self.quoteString(tablename))
#        for self.connection:
#            
#        ret = self.connection.ExecuteSQL(sql)
#        return ret[0] if ret is not None else None
        return None

    def getSpatialRefInfo(self, srid):
#        sql = "SELECT ref_sys_name FROM spatial_ref_sys WHERE srid = %s" % self.quoteString(srid)
#        ret = self.connection.ExecuteSQL(sql)
        
#        return ret[0] if ret is not None else None
        return None

    def isVectorTable(self, table):
        if self.has_geometry_columns:
            schema, tablename = self.getSchemaTableName(table)
            sql = "SELECT * FROM geometry_columns WHERE upper(f_table_name) = upper(%s)" % self.quoteString(
                tablename)
            c = self.connection.Execute(sql)
            ret = c.__len__()
            return ret is not None and ret > 0
        return True

    def isRasterTable(self, table):
        if self.has_geometry_columns and self.has_raster:
            schema, tablename = self.getSchemaTableName(table)
            if not tablename.endswith("_rasters"):
                return False

            sql = """SELECT *
                                        FROM layer_params AS r JOIN geometry_columns AS g
                                                ON upper(r.table_name||'_metadata') = upper(g.f_table_name)
                                        WHERE upper(r.table_name) = upper(REPLACE(%s, '_rasters', ''))""" % self.quoteString(
                tablename)
            ret = self.connection.ExecuteSQL(sql)
            return ret is not None and ret.__len__() > 0

        return False


    def createTable(self, table, field_defs, pkey):
        """ create ordinary table
                        'fields' is array containing field definitions
                        'pkey' is the primary key name
        """
        if len(field_defs) == 0:
            return False

        sql = "CREATE TABLE %s (" % self.quoteId(table)
        sql += ", ".join(field_defs)
        if pkey is not None and pkey != "":
            sql += ", PRIMARY KEY (%s)" % self.quoteId(pkey)
        sql += ")"

        self.connection.ExecuteSQL(sql)
        return True

    def deleteTable(self, table):
        """ delete table from the database """
        if self.isRasterTable(table):
            return False

        c = self.connection
        sql = "DROP TABLE %s" % self.quoteId(table)
        c.ExecuteSQL(sql)
        schema, tablename = self.getSchemaTableName(table)
        sql = str("DELETE FROM geometry_columns WHERE upper(f_table_name) = upper(%s)" % self.quoteString(tablename))
        c.ExecuteSQL(sql)


    def emptyTable(self, table):
        """ delete all rows from table """
        if self.isRasterTable(table):
            return False

        sql = str("DELETE FROM %s" % self.quoteId(table))
        self.connection.ExecuteSQL(sql)

    def renameTable(self, table, new_table):
        """ rename a table """
        schema, tablename = self.getSchemaTableName(table)
        if new_table == tablename:
            return

        if self.isRasterTable(table):
            return False

        c = self.connection

        sql = str("ALTER TABLE %s RENAME TO %s" % (self.quoteId(table), self.quoteId(new_table)))
        c.ExecuteSQL(sql)

        # update geometry_columns
        if self.has_geometry_columns:
            sql = str("UPDATE geometry_columns SET f_table_name = %s WHERE upper(f_table_name) = upper(%s)" % (
                self.quoteString(new_table), self.quoteString(tablename)))
            c.ExecuteSQL(sql)


    def moveTable(self, table, new_table, new_schema=None):
        return self.renameTable(table, new_table)

    def createView(self, view, query):
        sql = str("CREATE VIEW %s AS %s" % (self.quoteId(view), query))
        self.connection.ExecuteSQL(sql)

    def deleteView(self, view):
        c = self.connection

        sql = str("DROP VIEW %s" % self.quoteId(view))
        c.ExecuteSQL(sql)

        # update geometry_columns
        if self.has_geometry_columns:
            sql = str("DELETE FROM geometry_columns WHERE f_table_name = %s" % self.quoteString(view))
            c.ExecuteSQL(sql)

    def renameView(self, view, new_name):
        """ rename view """
        return self.renameTable(view, new_name)

    def createSpatialView(self, view, query):
        self.createView(view, query)
        # get type info about the view
        sql = str("PRAGMA table_info(%s)" % self.quoteString(view))
        c = self.connection.ExecuteSQL(sql )
        geom_col = None
        for r in c:        
            if r[2].upper() in ('POINT', 'LINESTRING', 'POLYGON',
                                'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON'):
                geom_col = r[1]
                break
        if geom_col is None:
            return

        # get geometry type and srid
        sql = str("SELECT geometrytype(%s), srid(%s) FROM %s LIMIT 1" % (self.quoteId(geom_col), self.quoteId(geom_col), self.quoteId(view)))
        r = self.connection.ExecuteSQL(sql )
        if r is None:
            return

        gtype, gsrid = r
        gdim = 'XY'
        if ' ' in gtype:
            zm = gtype.split(' ')[1]
            gtype = gtype.split(' ')[0]
            gdim += zm
        try:
            wkbType = ('POINT', 'LINESTRING', 'POLYGON', 'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON').index(gtype) + 1
        except:
            wkbType = 0
        if 'Z' in gdim:
            wkbType += 1000
        if 'M' in gdim:
            wkbType += 2000
        
        sql = str("""INSERT INTO geometry_columns (f_table_name, f_geometry_column, geometry_type, coord_dimension, srid, spatial_index_enabled)
                                        VALUES (%s, %s, %s, %s, %s, 0)""" % (self.quoteId(view), self.quoteId(geom_col), wkbType, len(gdim), gsrid))
        self.connection.ExecuteSQL(sql)

    def runVacuum(self):
        """ run vacuum on the db """
        self.connection.ExecuteSQL("VACUUM")


    def addTableColumn(self, table, field_def):
        """ add a column to table """
        sql = str("ALTER TABLE %s ADD %s" % (self.quoteId(table), field_def))
        self.connection.ExecuteSQL(sql)

    def deleteTableColumn(self, table, column):
        """ delete column from a table """
        if not self.isGeometryColumn(table, column):
            return False  # column editing not supported

        # delete geometry column correctly
        schema, tablename = self.getSchemaTableName(table)
        sql = str("SELECT DiscardGeometryColumn(%s, %s)" % (self.quoteString(tablename), self.quoteString(column)))
        self.connection.ExecuteSQL(sql)

    def updateTableColumn(self, table, column, new_name, new_data_type=None, new_not_null=None, new_default=None):
        return False  # column editing not supported

    def renameTableColumn(self, table, column, new_name):
        """ rename column in a table """
        return False  # column editing not supported

    def setColumnType(self, table, column, data_type):
        """ change column type """
        return False  # column editing not supported

    def setColumnDefault(self, table, column, default):
        """ change column's default value. If default=None drop default value """
        return False  # column editing not supported

    def setColumnNull(self, table, column, is_null):
        """ change whether column can contain null values """
        return False  # column editing not supported

    def isGeometryColumn(self, table, column):
        c = self.connection
        schema, tablename = self.getSchemaTableName(table)
        sql = str("SELECT count(*) > 0 FROM geometry_columns WHERE upper(f_table_name) = upper(%s) AND upper(f_geometry_column) = upper(%s)" % (
            self.quoteString(tablename), self.quoteString(column)))
        c = c.ExecuteSQL(sql)
        return c[0] == 't'

    def addGeometryColumn(self, table, geom_column='geometry', geom_type='POINT', srid=-1, dim=2):
        schema, tablename = self.getSchemaTableName(table)
        sql = str("SELECT AddGeometryColumn(%s, %s, %d, %s, %s)" % (
            self.quoteString(tablename), self.quoteString(geom_column), srid, self.quoteString(geom_type), dim))
        self.connection.ExecuteSQL(sql)

    def deleteGeometryColumn(self, table, geom_column):
        return self.deleteTableColumn(table, geom_column)


    def addTableUniqueConstraint(self, table, column):
        """ add a unique constraint to a table """
        return False  # constraints not supported

    def deleteTableConstraint(self, table, constraint):
        """ delete constraint in a table """
        return False  # constraints not supported


    def addTablePrimaryKey(self, table, column):
        """ add a primery key (with one column) to a table """
        sql = str("ALTER TABLE %s ADD PRIMARY KEY (%s)" % (self.quoteId(table), self.quoteId(column)))
        self.connection.ExecuteSQL(sql)


    def createTableIndex(self, table, name, column, unique=False):
        """ create index on one column using default options """
        unique_str = "UNIQUE" if unique else ""
        sql = str("CREATE %s INDEX %s ON %s (%s)" % (
            unique_str, self.quoteId(name), self.quoteId(table), self.quoteId(column)))
        self.conncetion.ExecuteSQL(str(sql))

    def deleteTableIndex(self, table, name):
        schema, tablename = self.getSchemaTableName(table)
        sql = str("DROP INDEX %s" % self.quoteId((schema, name)))
        self.connection.ExecuteSQL(sql)

    def createSpatialIndex(self, table, geom_column='geometry'):
        if self.isRasterTable(table):
            return False

        schema, tablename = self.getSchemaTableName(table)
        sql = str("SELECT CreateSpatialIndex(%s, %s)" % (self.quoteString(tablename), self.quoteString(geom_column)))
        self.connection.ExecuteSQL(sql)

    def deleteSpatialIndex(self, table, geom_column='geometry'):
        if self.isRasterTable(table):
            return False

        schema, tablename = self.getSchemaTableName(table)
        try:
            sql = str("SELECT DiscardSpatialIndex(%s, %s)" % (self.quoteString(tablename), self.quoteString(geom_column)))
            self.connection.ExecuteSQL(sql)
        except DbError:
            sql = str("SELECT DeleteSpatialIndex(%s, %s)" % (self.quoteString(tablename), self.quoteString(geom_column)))
            self.connection.ExecuteSQL(sql)
            # delete the index table
            idx_table_name = "idx_%s_%s" % (tablename, geom_column)
            self.deleteTable(idx_table_name)

    def hasSpatialIndex(self, table, geom_column='geometry'):
        if not self.has_geometry_columns or self.isRasterTable(table):
            return False
        c = self.connection()
        schema, tablename = self.getSchemaTableName(table)
        sql = str("SELECT spatial_index_enabled FROM geometry_columns WHERE upper(f_table_name) = upper(%s) AND upper(f_geometry_column) = upper(%s)" % (
            self.quoteString(tablename), self.quoteString(geom_column)))
        row = c.ExecuteSQL(sql)
        return row is not None and row[0] == 1

    def execution_error_types(self):
        return True

    def connection_error_types(self):
        return True

    # moved into the parent class: DbConnector._execute()
    # def _execute(self, cursor, sql):
    #       pass

    # moved into the parent class: DbConnector._execute_and_commit()
    #def _execute_and_commit(self, sql):
    #       pass

    # moved into the parent class: DbConnector._get_cursor()
    #def _get_cursor(self, name=None):
    #       pass

    # moved into the parent class: DbConnector._fetchall()
    #def _fetchall(self, c):
    #       pass

    # moved into the parent class: DbConnector._fetchone()
    #def _fetchone(self, c):
    #       pass

    # moved into the parent class: DbConnector._commit()
    #def _commit(self):
    #       pass

    # moved into the parent class: DbConnector._rollback()
    #def _rollback(self):
    #       pass

    # moved into the parent class: DbConnector._get_cursor_columns()
    #def _get_cursor_columns(self, c):
    #       pass

    def getSqlDictionary(self):
        from .sql_dictionary import getSqlDictionary

        sql_dict = getSqlDictionary()

        items = []
        for tbl in self.getTables():
            items.append(tbl[1])  # table name

            for fld in self.getTableFields(tbl[0]):
                items.append(fld[1])  # field name

        sql_dict["identifier"] = items
        return sql_dict

    def getQueryBuilderDictionary(self):
        from .sql_dictionary import getQueryBuilderDictionary

        return getQueryBuilderDictionary()
