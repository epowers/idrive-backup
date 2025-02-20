import datetime
import enum
from itertools import chain
import logging
import os
import sqlite3 as SQL
import socket
from typing import Optional


log = logging.getLogger(__name__.split('.',1)[0])

APP_NAME = 'idrive-backup'
DEFAULT_DB_NAME = 'index.db'

DB_SCHEMA = {
        'ibbackupset': {
            'columns': {
                'ITEM_ID': {
                    'column_number': 0,
                    'type': int,
                    'column_type': 'integer primary key autoincrement',
                    },
                'ITEM_NAME': {
                    'column_number': 1,
                    'type': str,
                    'column_type': 'char(50)',
                    },
                'ITEM_TYPE': {
                    'column_number': 2,
                    'type': str,
                    'column_type': 'char(2)',
                    },
                'ITEM_STATUS': {
                    'column_number': 3,
                    'type': str,
                    'column_type': 'char(50)',
                    },
                'ITEM_LMD': {
                    'column_number': 4,
                    'type': str,
                    'column_type': 'char(50)',
                    'default': '0',
                    },
                },
            'unique': [
                'ITEM_NAME',
                ],
            },
        'ibfolder': {
            'columns': {
                'DIRID': {
                    'column_number': 0,
                    'type': int,
                    'column_type': 'integer primary key autoincrement',
                    },
                'NAME': {
                    'column_number': 1,
                    'type': str,
                    'column_type': 'char(1024) not null',
                    },
                'DIR_LMD': {
                    'column_number': 2,
                    'type': str,
                    'column_type': 'char(256)',
                    },
                'DIR_SIZE': {
                    'column_number': 3,
                    'type': int,
                    'column_type': '__int64',
                    'default': 0,
                    },
                'DIR_COUNT': {
                    'column_number': 4,
                    'type': int,
                    'column_type': '__int64',
                    'default': 0,
                    },
                'DIR_PARENT': {
                    'column_number': 5,
                    'type': int,
                    'column_type': '__int64',
                    },
                },
            'unique': [
                'NAME',
                ],
            },
        'ibfile': {
            'columns': {
                'FILEID': {
                    'column_number': 0,
                    'type': int,
                    'column_type': 'integer primary key autoincrement',
                    },
                'DIRID': {
                    'column_number': 1,
                    'type': int,
                    'column_type': 'integer not null',
                    },
                'NAME': {
                    'column_number': 2,
                    'type': str,
                    'column_type': 'char(1024) not null',
                    },
                'FILE_LMD': {
                    'column_number': 3,
                    'type': datetime.datetime,
                    'column_type': 'datetime',
                    'default': 0,
                    },
                'FILE_SIZE': {
                    'column_number': 4,
                    'type': int,
                    'column_type': '__int64',
                    },
                'FOLDER_ID': {
                    'column_number': 5,
                    'type': str,
                    'column_type': 'char(1024)',
                    'default': "'-'",
                    },
                'ENC_NAME': {
                    'column_number': 6,
                    'type': str,
                    'column_type': 'char(1024)',
                    'default': "'-'",
                    },
                'BACKUP_STATUS': {
                    'column_number': 7,
                    'type': int,
                    'column_type': 'integer',
                    'default': 0,
                    },
                'CHECKSUM': {
                    'column_number': 8,
                    'type': str,
                    'column_type': 'char(256)',
                    'default': "'-'",
                    },
                'LAST_UPDATED': {
                    'column_number': 9,
                    'type': datetime.datetime,
                    'column_type': 'datetime',
                    'default': 'CURRENT_TIMESTAMP',
                    },
                },
            'foreign key': {
                'columns': [
                    'DIRID',
                    ],
                'references': {
                    'ibfolder': [
                        'DIRID',
                        ],
                    },
                },
            'unique': [
                'DIRID',
                'NAME',
                ],
            'indices': {
                'ibfile_name': {
                    'table': 'ibfile',
                    'columns': [
                        'NAME',
                        ],
                    },
                },
            },
        'DirEnt': {
            'columns': {
                'id': {
                    'column_name': 0,
                    'type': int,
                    'column_type': 'integer primary key',
                    },
                'path': {
                    'column_name': 1,
                    'type': str,
                    'column_type': 'text not null unique',
                    },
                'type': {
                    'column_name': 2,
                    'type': int,
                    'column_type': 'integer',
                    },
                'inode': {
                    'column_name': 3,
                    'type': int,
                    'column_type': 'integer',
                    },
                'mode': {
                    'column_name': 4,
                    'type': int,
                    'column_type': 'integer',
                    },
                'dev': {
                    'column_name': 5,
                    'type': int,
                    'column_type': 'integer',
                    },
                'nlink': {
                    'column_name': 6,
                    'type': int,
                    'column_type': 'integer',
                    },
                'uid': {
                    'column_name': 7,
                    'type': int,
                    'column_type': 'integer',
                    },
                'gid': {
                    'column_name': 8,
                    'type': int,
                    'column_type': 'integer',
                    },
                'size': {
                    'column_name': 9,
                    'type': int,
                    'column_type': 'integer',
                    },
                'atime': {
                    'column_name': 10,
                    'type': int,
                    'column_type': 'integer',
                    },
                'mtime': {
                    'column_name': 11,
                    'type': int,
                    'column_type': 'integer',
                    },
                'ctime': {
                    'column_name': 12,
                    'type': int,
                    'column_type': 'integer',
                    },
                'atime_ns': {
                    'column_name': 13,
                    'type': int,
                    'column_type': 'integer',
                    },
                'mtime_ns': {
                    'column_name': 14,
                    'type': int,
                    'column_type': 'integer',
                    },
                'ctime_ns': {
                    'column_name': 15,
                    'type': int,
                    'column_type': 'integer',
                    },
                'blocks': {
                    'column_name': 16,
                    'type': int,
                    'column_type': 'integer',
                    },
                'rdev': {
                    'column_name': 17,
                    'type': int,
                    'column_type': 'integer',
                    },
                #'flags': {
                #    'column_name': 18,
                #    'type': int,
                #    'column_type': 'integer',
                #    },
                },
            },
    }

class FileStatus(enum.IntEnum):
    DEFAULT = -1
    ERROR = -2
    SCANNED = 0
    DIRTY = 1


__hostname = None

def get_local_host():
    global __hostname
    if __hostname is None:
        __hostname = socket.gethostname()
    return __hostname


__cache_dir = None

def __get_cache_dir(create=False):
    global __cache_dir
    if __cache_dir is None:
        cache_home = os.getenv('XDG_CACHE_HOME', None) or os.path.expanduser('~/.cache')
        cache_home = os.path.abspath(cache_home)
        cache_dir = os.path.join(cache_home, APP_NAME)
        if os.path.isdir(cache_dir):
            __cache_dir = cache_dir
        elif create:
            if not os.path.isdir(cache_home):
                os.mkdir(cache_home)
            os.mkdir(cache_dir)
            __cache_dir = cache_dir
    return __cache_dir


__db_name = None

def __get_db_name(db_name=None, host=None, device_id=None):
    # if a db name was specified, always use that.
    if __db_name:
        return __db_name

    # if host and/or device, construct a db name.
    if host or device_id:
        return '.'.join(filter(None, (host, device_id))) + '.db'

    # otherwise default
    return DEFAULT_DB_NAME


def __db_create(db_name, db_dir=None):
    if not db_dir:
        db_dir = __get_cache_dir(True)
    db_path = os.path.join(db_dir, db_name)
    if not os.path.isfile(db_path):
        #tmp_db_path = os.path.join(db_dir, DEFAULT_DB_NAME)
        #if os.path.isfile(tmp_db_path):
        #    os.remove(tmp_db_path)
        #conn = SQL.connect(tmp_db_path)
        conn = SQL.connect(db_path)
        __db_create_tables(conn)
        #conn.close()
        #if os.path.isfile(db_path):
        #    os.remove(tmp_db_path)
        #else:
        #    os.rename(tmp_db_path, db_path)


def db_init(db_name=None, host=None, device_id=None):
    # set the current db name
    global __db_name
    __db_name = db_name

    # get the default db name
    db_name = __get_db_name(host=host, device_id=device_id)

    # create the default db
    __db_create(db_name)


__db_connections = dict()

def db_get_conn(host=None, device_id=None):
    key = (str(host) if host is not None else None, str(device_id) if device_id else None)
    conn = __db_connections.get(key)
    if conn is None:
        cache_dir = __get_cache_dir()
        db_name = __get_db_name(host=host, device_id=device_id)
        db_path = os.path.join(cache_dir, db_name)
        if not os.path.isfile(db_path):
            raise
        else:
            conn = SQL.connect(db_path)
    return conn

def db_cursor(host=None, device_id=None):
    conn = db_get_conn(host=host, device_id=device_id)
    cursor = conn.cursor()
    return cursor


def __db_create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE files ('''
        ''' host text not null,'''
        ''' device_id text default "" not null,'''
        ''' folder text default "" not null,'''
        ''' filename text default "" not null,'''
        ''' code integer default -1 not null CHECK(typeof(code) = "integer"),'''
        ''' ino integer default -1 not null CHECK(typeof(ino) = "integer"),'''
        ''' dev integer default -1 not null CHECK(typeof(dev) = "integer"),'''
        ''' size integer default -1 not null CHECK(typeof(size) = "integer"),'''
        ''' mtime real default -1 not null CHECK(typeof(mtime) = "real"),'''
        ''' md5 text )''') # TODO: sql 3.37.0+ supports STRICT
    cursor.execute('''CREATE INDEX idx_files_path ON files (folder ASC, filename ASC)''')
    cursor.execute('''CREATE INDEX idx_files_host ON files (host ASC)''')
    cursor.execute('''CREATE INDEX idx_files_device_id ON files (device_id ASC)''')
    cursor.execute('''CREATE INDEX idx_files_folder ON files (folder ASC)''')
    cursor.execute('''CREATE INDEX idx_files_filename ON files (filename ASC)''')
    conn.commit()


def __folder_path(folder):
    assert folder.startswith('/'), folder
    if not folder.endswith('/'):
        folder = folder + '/'
    return folder


def db_list_device_ids_by_host():
    cursor = db_cursor()
    fields = ('host', 'device_id')
    result = db_cursor_select_fetchall_files(cursor, fields=fields, distinct=True)
    hosts = dict()
    for host, device_id in result:
        if host not in hosts:
            hosts[host] = []
        hosts[host].append(device_id)
    return hosts


def db_filter_files_by_status(status, fields: tuple, host=None, device_id=None):
    assert host
    if device_id: raise NotImplemented
    cursor = db_cursor(host=host, device_id=device_id)
    fields = ','.join(fields)
    cursor.execute('''SELECT {fields} FROM files WHERE host = :host AND filename != "" AND code = :code'''.format(fields=fields), dict(host=host, code=status))
    return cursor


def db_cursor_select_fetchall_files(cursor, fields: tuple, where: Optional[dict] = None, distinct: bool = False):
    fields = ','.join(fields)
    conditions = where and ' AND '.join(map(lambda key: f'{key} = :{key}', where.keys()))
    cursor.execute('''SELECT {distinct} {fields} FROM files {where}'''.format(
        distinct = 'DISTINCT' if distinct else '',
        fields = fields,
        where = conditions and f'WHERE {conditions}' or '',
    ), where or {})
    result = cursor.fetchall()
    return result


def __db_cursor_insert_file(cursor, data: dict):
    primary_keys = set(('host', 'device_id', 'folder', 'filename'))
    keys = set(data.keys()) & primary_keys
    columns = ','.join(keys)
    variables = ','.join(map(lambda key: ':' + key, keys))
    conditions = ' AND '.join(map(lambda key: f'{key} = :{key}', keys))
    cursor.execute(
        '''INSERT INTO files ({columns}) '''
        '''SELECT {variables} WHERE NOT EXISTS '''
        '''(SELECT {columns} FROM files WHERE {conditions})'''
        ''.format(columns=columns, variables=variables, conditions=conditions), data)

    keys = set(data.keys()) - primary_keys
    if keys:
        predicates = ', '.join(map(lambda key: f'{key} = :{key}', keys))
        cursor.execute('''UPDATE files SET {predicates} WHERE {conditions}'''.format(predicates=predicates, conditions=conditions), data)


def db_cursor_select_fetchone_file(cursor, fields: tuple, where: dict):
    fields = ','.join(fields)
    conditions = ' AND '.join(map(lambda key: f'{key} = :{key}', where.keys()))
    cursor.execute('''SELECT {fields} FROM files WHERE {conditions} LIMIT 1'''.format(fields=fields, conditions=conditions), where)
    result = cursor.fetchone()
    return result[0] if result is not None else None


def db_cursor_update_file(cursor, data: dict, where: dict):
    conditions = ' AND '.join(map(lambda key: f'{key} = :{key}', where.keys()))
    predicates = ', '.join(map(lambda key: f'{key} = :{key}', data.keys()))
    values = {**data, **where}
    cursor.execute('''UPDATE files SET {predicates} WHERE {conditions}'''.format(predicates=predicates, conditions=conditions), values)


def db_cursor_insert_file(cursor, folder, filename, host=None, device_id=None, st_info=None, size=None, mtime=None):
    '''Stat file and add to database.'''
    assert host
    assert st_info is not None or (size is not None and mtime is not None)
    assert folder and filename
    data = dict(
        host=host,
        folder=__folder_path(folder),
        filename=filename,
        code=FileStatus.DEFAULT,
    )
    if device_id:
        data.update(dict(
            device_id=device_id,
        ))
    if st_info is not None:
        data.update(dict(
            ino=st_info.st_ino,
            dev=st_info.st_dev,
            size=st_info.st_size,
            mtime=st_info.st_mtime,
        ))
    else:
        data.update(dict(
            size=size,
            mtime=mtime,
        ))
    __db_cursor_insert_file(cursor, data)


def db_cursor_insert_folder(cursor, folder, host=None, device_id=None):
    '''Add folder into database.'''
    assert host
    assert folder
    data = dict(
        host=host,
        folder=__folder_path(folder),
    )
    if device_id:
        data.update(dict(
            device_id=device_id,
        ))
    __db_cursor_insert_file(cursor, data)


def db_any_file_path(filename, folder=None, host=None, device_id=None, **kwargs):
    '''Find any matching file by path and return bool.'''
    assert host
    fields = ('size',)
    where = dict(
        host=host,
        filename=filename,
        **kwargs,
    )
    if folder:
        where.update(dict(
            folder=__folder_path(folder),
        ))
    if device_id:
        where.update(dict(
            device_id=device_id,
        ))
    cursor = db_cursor(host=host, device_id=device_id)
    result = db_cursor_select_fetchone_file(cursor, fields, where)
    return result is not None


def db_get_folder_size(folder, host=None, device_id=None):
    '''Find a matching folder and return size.'''
    assert host
    fields = ('size',)
    where = dict(
        host=host,
        folder=__folder_path(folder),
        filename="",
    )
    if device_id:
        where.update(dict(
            device_id=device_id,
        ))
    cursor = db_cursor(host=host, device_id=device_id)
    result = db_cursor_select_fetchone_file(cursor, fields, where)
    return result


def db_fetch_next_folder(host=None, device_id=None):
    '''Find a matching folder and return path.'''
    assert host
    fields = ('folder',)
    where = dict(
        host=host,
        filename="",
        code=FileStatus.DEFAULT,
    )
    if device_id:
        where.update(dict(
            device_id=device_id,
        ))
    cursor = db_cursor(host=host, device_id=device_id)
    result = db_cursor_select_fetchone_file(cursor, fields, where)
    return result


def db_has_folder(folder, host=None, device_id=None):
    '''Find a matching folder and return bool.'''
    assert host
    size = db_get_folder_size(folder, host=host, device_id=device_id)
    return size is not None


def db_insert_folder(folder, host=None, device_id=None):
    '''Add folder into database.'''
    cursor = db_cursor(host=host, device_id=device_id)
    db_cursor_insert_folder(cursor, folder, host=host, device_id=device_id)
    cursor.connection.commit()


def db_cursor_update_folder_size(cursor, folder, size, host=None, device_id=None):
    assert host
    # update the size of the folder in the database with the number of files/folders
    data = dict(
        size=size,
    )
    where = dict(
        host=host,
        folder=__folder_path(folder),
        filename="",
    )
    if device_id:
        where.update(dict(
            device_id=device_id,
        ))
    db_cursor_update_file(cursor, data, where)


def db_cursor_update_folder_status(cursor, folder, status, host=None, device_id=None):
    assert host
    # update the code of the folder in the database with the number of files/folders
    data = dict(
        code=status, # field is named code, status may involve other field changes
    )
    where = dict(
        host=host,
        folder=__folder_path(folder),
        filename="",
    )
    if device_id:
        where.update(dict(
            device_id=device_id,
        ))
    db_cursor_update_file(cursor, data, where)


def db_update_file_status(folder, filename, status, host=None, device_id=None):
    '''Update file status.'''
    assert host
    cursor = db_cursor(host=host, device_id=device_id)
    data = dict(
        code=status, # field is named code, status may involve other field changes
    )
    where = dict(
        host=host,
        folder=__folder_path(folder),
        filename=filename,
    )
    if device_id:
        where.update(dict(
            device_id=device_id,
        ))
    db_cursor_update_file(cursor, data, where)
    cursor.connection.commit()


def db_update_file_path_md5(path):
    raise NotImplemented


def create_table(cursor, table):
    columns = DB_SCHEMA[table]['columns']
    indices = DB_SCHEMA[table].get('indices', [])
    unique = DB_SCHEMA[table].get('unique')
    foreign_key = DB_SCHEMA[table].get('foreign key')

    cursor.executemany([
        '''CREATE TABLE {table} ({columns} {foreign_key} {unique})'''.format(
            table = table,
            columns = ','.join(map(lambda name: '''{name} {type} {default}'''.format(
                name = name,
                type = columns[name]['column_type'],
                default = 'default {}'.format(columns[name]['default']) if 'default' in columns[name] else '',
                ), columns.keys())),
            foreign_key = '''foreign key ({columns}) references {references}'''.format(
                columns = ','.join(foreign_key['columns']),
                references = ','.join(map(lambda table: '''{table} ({columns})'''.format(
                    table = table,
                    columns = ','.join(foreign_key['references'][table])), foreign_key['references'].keys())),
                ) if foreign_key else '',
            unique = '''unique ({columns})'''.format(
                columns = ','.join(unique),
                ) if unique else '',
            ),
        ] + list(map(lambda name:
        '''CREATE INDEX {name} ON {table} ({columns})'''.format(
            name = name,
            table = table,
            columns = ','.join(indices[name]),
            ), indices.keys())))
    cursor.connection.commit()


def idrive_db_init(db_name=None, db_dir=None, host=None, device_id=None):
    # set the current db name
    global __db_name
    __db_name = db_name

    # get the default db name
    db_name = __get_db_name(host=host, device_id=device_id)

    # create the default db
    if not db_dir:
        db_dir = __get_cache_dir(True)
    db_path = os.path.join(db_dir, db_name)
    if not os.path.isfile(db_path):
        connection = SQL.connect(db_path)
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE files ('''
            ''' host text not null,'''
            ''' device_id text default "" not null,'''
            ''' folder text default "" not null,'''
            ''' filename text default "" not null,'''
            ''' code integer default -1 not null CHECK(typeof(code) = "integer"),'''
            ''' ino integer default -1 not null CHECK(typeof(ino) = "integer"),'''
            ''' dev integer default -1 not null CHECK(typeof(dev) = "integer"),'''
            ''' size integer default -1 not null CHECK(typeof(size) = "integer"),'''
            ''' mtime real default -1 not null CHECK(typeof(mtime) = "real"),'''
            ''' md5 text )''') # TODO: sql 3.37.0+ supports STRICT
        cursor.execute('''CREATE INDEX idx_files_path ON files (folder ASC, filename ASC)''')
        cursor.execute('''CREATE INDEX idx_files_host ON files (host ASC)''')
        cursor.execute('''CREATE INDEX idx_files_device_id ON files (device_id ASC)''')
        cursor.execute('''CREATE INDEX idx_files_folder ON files (folder ASC)''')
        cursor.execute('''CREATE INDEX idx_files_filename ON files (filename ASC)''')
        connection.commit()

        # create table
        cursor.executemany([
            '''CREATE TABLE IF NOT EXISTS DirEnt ({})'''.format(
                ", ".join(" ".join(e) for e in DIRENT_TABLE_SCHEMA)),
            '''CREATE UNIQUE INDEX IF NOT EXISTS DirEnt_path_idx ON DirEnt (path)''',
            '''CREATE INDEX IF NOT EXISTS DirEnt_size_idx ON DirEnt (size)''',
            ])
        connection.commit()


def __idrive_db_select(cursor, table, fields: Optional[tuple] = None):
    columns = DB_SCHEMA[table]['columns'].keys()
    if fields is not None:
        columns = list(filter(lambda name: name in fields, columns))
    cursor.execute('''SELECT {columns} FROM {table}'''.format(
        columns = ','.join(columns),
        table = table,
    ))

def idrive_db_fetchall(cursor, table, fields: Optional[tuple] = None):
    __idrive_db_select(cursor, table, fields)
    result = cursor.fetchall()
    return result

def idrive_db_select_files(cursor):
    ibfile = DB_SCHEMA['ibfile']['columns'].keys()
    ibfile = list(map(lambda column: 'ibfile.{column}'.format(column=column), ibfile))
    ibfolder = DB_SCHEMA['ibfolder']['columns'].keys()
    ibfolder = list(map(lambda column: 'ibfolder.{column}'.format(column=column), ibfolder))
    cursor.execute('''SELECT {columns} FROM ibfile JOIN ibfolder ON ibfile.DIRID = ibfolder.DIRID'''.format(
        columns = ','.join(chain(ibfile, ibfolder)),
    ))
    return map(lambda row: dict(zip(ibfile + ibfolder, row)), cursor)
