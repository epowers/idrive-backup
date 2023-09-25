import logging
import requests


log = logging.getLogger(__name__.split('.',1)[0])


__idrive_host = "evs.idrive.com"

def idrive_get_host():
    return __idrive_host


__idrive_session = None

def idrive_get_session():
    global __idrive_session
    if __idrive_session is None:
        __idrive_session = requests.Session()
    return __idrive_session


__idrive_uid = __idrive_pwd = __idrive_web_api_server = __idrive_device_id = None

def idrive_set_device_id(device_id):
    global __idrive_device_id
    __idrive_device_id = device_id

def idrive_session_post(command=None, data=None, **params):
    params = dict(params)
    uid, pwd, host, device_id = params.pop('uid', __idrive_uid), params.pop('pwd', __idrive_pwd), params.pop('host', __idrive_web_api_server), params.pop('device_id', __idrive_device_id)

    session = idrive_get_session()
    url = f"https://{host}/evs/{command}"
    params = params or None
    data = dict(data or {})
    if device_id:
        data.update(dict(device_id=device_id))
    log.debug('idrive_session_post(): request: {}'.format(dict(host=host,command=command,params=params,data=data)))
    data.update(dict(uid=uid, pwd=pwd, json='yes'))

    response = session.post(url, params=params, data=data)

    response.raise_for_status()
    assert 'application/json' in response.headers.get('content-type',''), dict(status_code=response.status_code, content=response.content)

    result = response.json()
    log.debug('idrive_session_post(): response: {} {}'.format(url, result))
    message = result.get('message', None)
    assert message == "SUCCESS", result
    if 'contents' in result:
        contents = result['contents']
        assert isinstance(contents, list), result
        return contents
    return result


def idrive_getServerAddress(**kwargs):
    host = __idrive_host
    command = "getServerAddress"
    result = idrive_session_post(host=host, command=command, device_id=None, **kwargs)
    host = result.get('webApiServer', None)
    assert host, result
    return host


def idrive_validateAccount(**kwargs):
    command = 'validateAccount'
    result = idrive_session_post(command=command, device_id=None, **kwargs)
    desc = result.get('desc', None)
    assert desc == "VALID ACCOUNT", result


def idrive_login(uid, pwd):
    global __idrive_web_api_server, __idrive_uid, __idrive_pwd
    try:
        host = idrive_getServerAddress(uid=uid, pwd=pwd)
        idrive_validateAccount(host=host, uid=uid, pwd=pwd)
        __idrive_web_api_server, __idrive_uid, __idrive_pwd = host, uid, pwd
        return True
    except:
        return False


def idrive_listDevices(**kwargs):
    command = 'listDevices'
    contents = idrive_session_post(command=command, device_id=None, **kwargs)
    return contents


def idrive_browseFolder(device_id, path=None):
    path = path or '/'
    command = 'browseFolder'
    data = dict(
            p=path,
            # p = "/mnt/Backup",
            device_id=device_id,
            #version=0,
            )
    contents = idrive_session_post(command=command, data=data)
    return contents
