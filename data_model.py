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

from ..data_model import TableDataModel, SqlResultModel

class SLTableDataModel(TableDataModel):
    def __init__(self, table, parent=None):
        TableDataModel.__init__(self, table, parent)

        fields_txt = ", ".join(self.fields)
        table_txt = self.db.quoteId((self.table.schemaName(), self.table.name))

        # run query and get results
        sql = str("SELECT %s FROM %s" % (str(fields_txt), str(table_txt)))
        c = self.db.connection
        self.resdata = c.ExecuteSQL(sql)
        data = []

        for feat in self.resdata:
            count = feat.GetFieldCount() + 1
            fields = ()
            for i in range(count):
                if i == 0:
                    fields += (feat.GetFID(), )
                elif i == 1:
                    fields += (None, )
                else:
                    fields += (feat.GetField(i-1), )
            data.append(fields)
            
        self.resdata = data
        self.fetchedFrom = 0
        self.fetchedCount = len(self.resdata)


    def _sanitizeTableField(self, field):
        # get fields, ignore geometry columns
        dataType = field.dataType.upper()
        if dataType[:5] == "MULTI": dataType = dataType[5:]
        if dataType[-3:] == "25D": dataType = dataType[:-3]
        if dataType[-10:] == "COLLECTION": dataType = dataType[:-10]
#        if dataType in ["POINT", "LINESTRING", "POLYGON", "GEOMETRY"]:
#            return u'GeometryType(%s)' % self.db.quoteId(field.name)
        return self.db.quoteId(field.name)

    def rowCount(self, index=None):
        return self.fetchedCount


class SLSqlResultModel(SqlResultModel):
    pass
