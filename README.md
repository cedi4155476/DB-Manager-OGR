# OGR implementation of QGis DBManager plugin
## Introduction

This Python code try to implement the OGR part of the QGis DBManager plugin. DBManager plugin is a good tool from QGis with which you can easily manage your databases and create your own queries which can be dynamically added to QGis maps.

For the moment, DBManager plugin is only able to connect to PostGIS and Spatialite databases. If you want to manage your Geopackage repository, you can (try) to do this with this code implementation.

The code base of this implementation was the SpatiaLite one. You can only read the Databases and add them to QGis. Databases are added through Drag and Drop.

Expect bugs !
## Installation

To install DBManager geopackage plugin, you just have to clone the git repository in a directory named geopackage in the db_plugins directory of the db_manager installation.

Just add the geopackage directory in the db-manager/db_plugins directory.

## Limitations

* You can not see the Vectors in DBManager.
* You can not edit the database or it will crash.
* Tests have been done with QGis 2.8.2.
* Some things could not have been well tested, particularly everything.
* Tests have been done against some Geopackages sample databases.
* Code has been tested only under Linux but as it is Python code, I hope it will also works under other OS.

## Bug reports

For the moment, use the "issues" tool of GitHub to report bugs.
## Main goal

My main goal is that this code can be incorporated in the official QGis source code repository. Once this has been done, the code upgrades will take place there.
## License

This code is released under the GNU GPLv2 license. Read headers code for more information.