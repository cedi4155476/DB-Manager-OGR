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

from PyQt4.QtGui import QApplication

from ..info_model import DatabaseInfo
from ..html_elems import HtmlTable


class SLDatabaseInfo(DatabaseInfo):
    def __init__(self, db):
        self.db = db

    def connectionDetails(self):
        tbl = [
            (QApplication.translate("DBManagerPlugin", "Filename:"), self.db.connector.dbname)
        ]
        return HtmlTable(tbl)

    def generalInfo(self):
        info = self.db.connector.getInfo()
        tbl = [
            (QApplication.translate("DBManagerPlugin", "GDAL Version:"), info[0])
        ]
        return HtmlTable(tbl)

    def privilegesDetails(self):
        return None
