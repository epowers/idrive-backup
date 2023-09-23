import logging
import os
import requests
import sqlite3 as SQL
import socket
import sys
import xml.dom.minidom as XML


APP_NAME = 'idrive-backup'
DB_NAME = 'index.db'

log = logging.getLogger(APP_NAME)


########
#
# SQLite
#
########

__hostname = socket.gethostname()

__cache_dir = None

def __get_cache_dir():
    global __cache_dir
    if __cache_dir is None:
        cache_home = os.getenv('XDG_CACHE_HOME', None) or os.path.expanduser('~/.cache')
        cache_home = os.path.abspath(cache_home)
        if not os.path.isdir(cache_home):
            os.mkdir(cache_home)
        cache_dir = os.path.join(cache_home, APP_NAME)
        if not os.path.isdir(cache_dir):
            os.mkdir(cache_dir)
        __cache_dir = cache_dir
    return __cache_dir


__db_name = None

def db_init(db_name=None):
    global __db_name
    if not db_name:
        db_name = DB_NAME

    cache_dir = __get_cache_dir()
    db_path = os.path.join(cache_dir, db_name)
    if not os.path.isfile(db_path):
        #tmp_db_path = os.path.join(cache_dir, DB_NAME)
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
    __db_name = db_name


__db_conn = None

def __db_get_conn():
    global __db_conn
    if __db_conn is None:
        cache_dir = __get_cache_dir()
        db_path = os.path.join(cache_dir, __db_name)
        if not os.path.isfile(db_path):
            raise
        else:
            conn = SQL.connect(db_path)
        __db_conn = conn
    return __db_conn

def __db_cursor():
    conn = __db_get_conn()
    cursor = conn.cursor()
    return cursor


def __db_create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE files ('''
        ''' device_id TEXT NOT NULL,'''
        ''' folder TEXT NOT NULL,'''
        ''' filename TEXT NOT NULL,'''
        ''' status INTEGER NOT NULL CHECK(typeof(status) = "INTEGER"),'''
        ''' ino INTEGER NOT NULL CHECK(typeof(ino) = "INTEGER"),'''
        ''' dev INTEGER NOT NULL CHECK(typeof(dev) = "INTEGER"),'''
        ''' size INTEGER NOT NULL CHECK(typeof(size) = "INTEGER"),'''
        ''' mtime REAL NOT NULL CHECK(typeof(mtime) = "REAL"),'''
        ''' md5 TEXT )''') # TODO: sql 3.37.0+ supports STRICT
    cursor.execute('''CREATE INDEX idx_files_path ON files (folder ASC, filename ASC)''')
    cursor.execute('''CREATE INDEX idx_files_device_id ON files (device_id ASC)''')
    cursor.execute('''CREATE INDEX idx_files_folder ON files (folder ASC)''')
    cursor.execute('''CREATE INDEX idx_files_filename ON files (filename ASC)''')
    conn.commit()


def __folder_path(folder):
    assert folder.startswith('/'), folder
    if not folder.endswith('/'):
        folder = folder + '/'
    return folder


def __db_insert_file(cursor, folder, filename, st_info):
    '''Stat file and add to database.'''
    folder = __folder_path(folder)
    device_id = __hostname
    ino, dev, size, mtime = st_info.st_ino, st_info.st_dev, st_info.st_size, st_info.st_mtime
    cursor.execute('''INSERT INTO files VALUES (:device_id,:folder,:filename,:ino,:dev,:size,:mtime,NULL)''',
        dict(device_id=device_id, folder=folder, filename=filename, ino=ino, dev=dev, size=size, mtime=mtime))


def __db_insert_remote_file(cursor, device_id, folder, filename, size, mtime):
    '''Add remote path info to database.'''
    folder = __folder_path(folder)
    ino, dev = -1, -1
    cursor.execute('''INSERT INTO files VALUES (:device_id,:folder,:filename,:ino,:dev,:size,:mtime,NULL)''',
        dict(device_id=device_id, folder=folder, filename=filename, ino=ino, dev=dev, size=size, mtime=mtime))


def __db_insert_folder(cursor, folder, device_id=None):
    '''Add folder into database.'''
    folder = __folder_path(folder)
    if device_id is None:
        device_id = __hostname
    filename, ino, dev, size, mtime = '', -1, -1, -1, -1
    cursor.execute('''INSERT INTO files VALUES (:device_id,:folder,:filename,:ino,:dev,:size,:mtime,NULL)''',
        dict(device_id=device_id, folder=folder, filename=filename, ino=ino, dev=dev, size=size, mtime=mtime))


def __db_any_file_path(path):
    '''Find any matching file by path and return bool.'''
    device_id = __hostname
    folder, filename = os.path.dirname(path), os.path.basename(path)
    folder = __folder_path(folder)
    cursor = __db_cursor()
    cursor.execute('''SELECT size FROM files WHERE device_id = :device_id AND folder = :folder'''
        ''' AND filename = :filename LIMIT 1''', dict(device_id=device_id, folder=folder, filename=filename))
    result = cursor.fetchone()
    return result is not None


def __db_get_folder_size(device_id, folder):
    '''Find a matching folder and return size.'''
    folder = __folder_path(folder)
    cursor = __db_cursor()
    cursor.execute('''SELECT size FROM files WHERE device_id = :device_id AND folder = :folder AND filename = ""'''
        ''' AND filename = ""  LIMIT 1''', dict(device_id=device_id, folder=folder))
    result = cursor.fetchone()
    return result[0] if result is not None else None


def __db_fetch_next_folder(device_id=None):
    '''Find a matching folder and return path.'''
    size = -1
    if device_id is None:
        device_id = __hostname

    cursor = __db_cursor()
    cursor.execute('''SELECT folder FROM files WHERE device_id = :device_id AND filename = "" AND size = :size LIMIT 1''',
        dict(device_id=device_id, size=size))
    result = cursor.fetchone()
    return result[0] if result is not None else None


def __db_has_folder(folder, device_id=None):
    '''Find a matching folder and return bool.'''
    folder = __folder_path(folder)
    if device_id is None:
        device_id = __hostname
    size = __db_get_folder_size(device_id, folder)
    return size is not None


def db_insert_folder(folder, device_id=None):
    '''Add folder into database.'''
    folder = __folder_path(folder)
    if device_id is None:
        device_id = __hostname
    cursor = __db_cursor()
    __db_insert_folder(cursor, folder, device_id=device_id)
    cursor.connection.commit()


def __db_update_folder_size(cursor, folder, size, device_id=None):
    # update the size of the folder in the database with the number of files/folders
    folder = __folder_path(folder)
    if device_id is None:
        device_id = __hostname
    cursor.execute('''UPDATE files SET size = :size WHERE device_id = :device_id AND folder = :folder AND filename = ""''',
        dict(device_id=device_id, folder=folder, size=size))


def __db_update_file_path_md5(path):
    raise NotImplemented


########
#
# IDrive
#
########

__idrive_session = None

def __idrive_get_session():
    global __idrive_session
    if __idrive_session is None:
        __idrive_session = requests.Session()
    return __idrive_session


__idrive_uid = __idrive_pwd = __idrive_web_api_server = __idrive_device_id = None

def __idrive_set_device_id(device_id):
    global __idrive_device_id
    __idrive_device_id = device_id

def __idrive_session_post(command=None, data=None, **params):
    params = dict(params)
    uid, pwd, host, device_id = params.pop('uid', __idrive_uid), params.pop('pwd', __idrive_pwd), params.pop('host', __idrive_web_api_server), params.pop('device_id', __idrive_device_id)

    session = __idrive_get_session()
    url = f"https://{host}/evs/{command}"
    params = params or None
    data = dict(data or {})
    if device_id:
        data.update(dict(device_id=device_id))
    log.debug('__idrive_session_post(): request: {}'.format(dict(host=host,command=command,params=params,data=data)))
    data.update(dict(uid=uid, pwd=pwd, json='yes'))

    response = session.post(url, params=params, data=data)

    response.raise_for_status()
    assert 'application/json' in response.headers.get('content-type',''), dict(status_code=response.status_code, content=response.content)

    result = response.json()
    log.debug('__idrive_session_post(): response: {} {}'.format(url, result))
    message = result.get('message', None)
    assert message == "SUCCESS", result
    if 'contents' in result:
        contents = result['contents']
        assert isinstance(contents, list), result
        return contents
    return result


def __idrive_getServerAddress(**kwargs):
    host = "evs.idrive.com"
    command = "getServerAddress"
    result = __idrive_session_post(host=host, command=command, device_id=None, **kwargs)
    host = result.get('webApiServer', None)
    assert host, result
    return host


def __idrive_validateAccount(**kwargs):
    command = 'validateAccount'
    result = __idrive_session_post(command=command, device_id=None, **kwargs)
    desc = result.get('desc', None)
    assert desc == "VALID ACCOUNT", result


def idrive_login(uid, pwd):
    global __idrive_web_api_server, __idrive_uid, __idrive_pwd
    try:
        host = __idrive_getServerAddress(uid=uid, pwd=pwd)
        __idrive_validateAccount(host=host, uid=uid, pwd=pwd)
        __idrive_web_api_server, __idrive_uid, __idrive_pwd = host, uid, pwd
        return True
    except:
        return False


def __idrive_listDevices(**kwargs):
    command = 'listDevices'
    contents = __idrive_session_post(command=command, device_id=None, **kwargs)
    return contents


def __idrive_browseFolder(device_id, path=None):
    path = path or '/'
    command = 'browseFolder'
    data = dict(
            p=path,
            # p = "/mnt/Backup",
            device_id=device_id,
            #version=0,
            )
    contents = __idrive_session_post(command=command, data=data)
    return contents
