import enum
import logging
import os
import sqlite3 as SQL
import socket


APP_NAME = 'idrive-backup'
DEFAULT_DB_NAME = 'index.db'

class FileStatus(enum.IntEnum):
    DEFAULT = -1
    ERROR = -2
    SCANNED = 0


log = logging.getLogger(__name__.split('.',1)[0])


__hostname = socket.gethostname()

def __get_host():
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

def __db_get_conn(host=None, device_id=None):
    key = (str(host) if host is not None else None, str(device_id) if device_id is not None else None)
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

def __db_cursor(host=None, device_id=None):
    host = host if host is not None else __hostname
    conn = __db_get_conn(host=host, device_id=device_id)
    cursor = conn.cursor()
    return cursor


def __db_create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE files ('''
        ''' host TEXT NOT NULL,'''
        ''' device_id TEXT DEFAULT "" NOT NULL,'''
        ''' folder TEXT DEFAULT "" NOT NULL,'''
        ''' filename TEXT DEFAULT "" NOT NULL,'''
        ''' code INTEGER DEFAULT -1 NOT NULL CHECK(typeof(code) = "integer"),'''
        ''' ino INTEGER DEFAULT -1 NOT NULL CHECK(typeof(ino) = "integer"),'''
        ''' dev INTEGER DEFAULT -1 NOT NULL CHECK(typeof(dev) = "integer"),'''
        ''' size INTEGER DEFAULT -1 NOT NULL CHECK(typeof(size) = "integer"),'''
        ''' mtime REAL DEFAULT -1 NOT NULL CHECK(typeof(mtime) = "real"),'''
        ''' md5 TEXT )''') # TODO: sql 3.37.0+ supports STRICT
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


def __db_cursor_insert_file(cursor, data: dict):
    keys, values = zip(*data.items())
    columns = ','.join(keys)
    variables = ','.join(map(lambda key: ':' + key, keys))
    cursor.execute('''INSERT INTO files ({columns}) VALUES ({variables})'''.format(columns=columns, variables=variables), data)


def __db_cursor_select_fetchone_file(cursor, fields: tuple, where: dict):
    fields = ','.join(fields)
    conditions = ' AND '.join(map(lambda key: f'{key} = :{key}', where.keys()))
    cursor.execute('''SELECT {fields} FROM files WHERE {conditions} LIMIT 1'''.format(fields=fields, conditions=conditions), where)
    result = cursor.fetchone()
    return result[0] if result is not None else None


def __db_cursor_update_file(cursor, data: dict, where: dict):
    conditions = ' AND '.join(map(lambda key: f'{key} = :{key}', where.keys()))
    predicates = ', '.join(map(lambda key: f'{key} = :{key}', data.keys()))
    values = {**data, **where}
    cursor.execute('''UPDATE files SET {predicates} WHERE {conditions}'''.format(predicates=predicates, conditions=conditions), values)


def __db_insert_file(cursor, folder, filename, host=None, device_id=None, st_info=None, size=None, mtime=None):
    '''Stat file and add to database.'''
    assert st_info is not None or (size is not None and mtime is not None)
    assert folder and filename
    data = dict(
        host=host if host is not None else __hostname,
        folder=__folder_path(folder),
        filename=filename,
        code=FileStatus.DEFAULT,
    )
    if device_id is not None:
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


def __db_insert_folder(cursor, folder, host=None, device_id=None):
    '''Add folder into database.'''
    assert folder
    data = dict(
        host=host if host is not None else __hostname,
        folder=__folder_path(folder),
    )
    if device_id is not None:
        data.update(dict(
            device_id=device_id,
        ))
    __db_cursor_insert_file(cursor, data)


def __db_any_file_path(path):
    '''Find any matching file by path and return bool.'''
    host = __hostname
    fields = ('size',)
    where = dict(
        host=host,
        folder=__folder_path(os.path.dirname(path)),
        filename=os.path.basename(path),
    )
    cursor = __db_cursor(host=host)
    result = __db_cursor_select_fetchone_file(cursor, fields, where)
    return result is not None


def __db_get_folder_size(host, folder, device_id=None):
    '''Find a matching folder and return size.'''
    fields = ('size',)
    where = dict(
        host=host,
        folder=__folder_path(folder),
        filename="",
    )
    if device_id is not None:
        where.update(dict(
            device_id=device_id,
        ))
    cursor = __db_cursor(host=host, device_id=device_id)
    result = __db_cursor_select_fetchone_file(cursor, fields, where)
    return result


def __db_fetch_next_folder(host=None, device_id=None):
    '''Find a matching folder and return path.'''
    host = host if host is not None else __hostname
    fields = ('folder',)
    where = dict(
        host=host,
        filename="",
        code=FileStatus.DEFAULT,
    )
    if device_id is not None:
        where.update(dict(
            device_id=device_id,
        ))
    cursor = __db_cursor(host=host, device_id=device_id)
    result = __db_cursor_select_fetchone_file(cursor, fields, where)
    return result


def __db_has_folder(folder, host=None, device_id=None):
    '''Find a matching folder and return bool.'''
    if host is None:
        host = __hostname
    size = __db_get_folder_size(host, folder, device_id=device_id)
    return size is not None


def db_insert_folder(folder, host=None, device_id=None):
    '''Add folder into database.'''
    cursor = __db_cursor(host=host, device_id=device_id)
    __db_insert_folder(cursor, folder, host=host, device_id=device_id)
    cursor.connection.commit()


def __db_update_folder_size(cursor, folder, size, host=None, device_id=None):
    # update the size of the folder in the database with the number of files/folders
    data = dict(
        size=size,
    )
    where = dict(
        host=host if host is not None else __hostname,
        folder=__folder_path(folder),
        filename="",
    )
    if device_id is not None:
        where.update(dict(
            device_id=device_id,
        ))
    __db_cursor_update_file(cursor, data, where)


def __db_update_folder_status(cursor, folder, status, host=None, device_id=None):
    # update the code of the folder in the database with the number of files/folders
    data = dict(
        code=status, # field is named code, status may involve other field changes
    )
    where = dict(
        host=host if host is not None else __hostname,
        folder=__folder_path(folder),
        filename="",
    )
    if device_id is not None:
        where.update(dict(
            device_id=device_id,
        ))
    __db_cursor_update_file(cursor, data, where)


def __db_update_file_path_md5(path):
    raise NotImplemented
