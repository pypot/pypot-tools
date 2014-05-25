#-*- coding:utf-8 -*-
#author:phanyoung@outlook.com
#date  :2014.02.14
#version:1.0

import sys, os, time, datetime
import traceback
sys.path.append("./gen-py")
import name_service_py
from name_service_py.nameservice import NameService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol


__DEBUG__ = True
__LOG__ = True

__LOCAL_LOG_METHOD__ = True
if __LOCAL_LOG_METHOD__:
    __LOG_LEVEL__ = 0
    log_level_dict = {"Debug":1, "Info":2, "Log":3, "Error":4, "Fatal":5}

    def printlog(logtype, logkey, logvalue):
        if (logtype in log_level_dict) and (log_level_dict[logtype] >= __LOCAL_LOG_METHOD__):
            sys.stderr.write("%s [%s]%s=%s\n" \
                             % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                logtype, logkey, logvalue))

__USE_ZK__ = True
if __USE_ZK__:

    class ZkCreateError(Exception):
        pass

    class ZkMonitor(object):
        def __init__(self, zk_server, zk_device, server_name):
            self.name_service = NameService(zk_server, zk_device)
            self.server_name = server_name

            if self.name_service.watch_service(server_name) == False:
                if __LOG__:
                    printlog("Fatal", 'watch zk %s, %s:%s'
                             % (server_name, zk_server, zk_device), 'error')
                raise ZkCreateError("watch zk %s , %s:%s,error"
                                    % (server_name, zk_server, zk_device))
            if self.name_service.start() == False:
                if __LOG__:
                    printlog("Fatal", 'zk start %s, %s:%s'
                             % (server_name, zk_server, zk_device), 'fail')
                raise ZkCreateError("start zk %s, %s:%s, error"
                                    % (server_name, zk_server, zk_device))

            if __LOG__:
                print 'zk start %s, %s:%s' % (server_name, zk_server, zk_device)
                printlog("Log", 'zk start %s, %s:%s'
                         % (server_name, zk_server, zk_device), 'OK')
            time.sleep(2)

        def Stop(self):
            self.name_service.stop()

        def GetServerInfo(self):
            ret_str, server_host, server_port = self.name_service.get_service_host_and_port(
                                             self.server_name, self.name_service.EPOLICY_RANDOM)
            retry = 0
            while ret_str != 'ok' and retry < 5:
                time.sleep(2)
                ret_str, server_host, server_port = self.name_service.get_service_host_and_port(
                                               self.server_name,self.name_service.EPOLICY_RANDOM)
                retry += 1
                printlog("Error", 'cannot get zk info','retry %d' % retry)
            return server_host, server_port



_DEFAULT_SERVER_TIMEOUT = 3000


class ThriftClientInitError(Exception):
    def __init__(self, err_info="Error init parameters for GeneralClient", *args):
        self.err = err_info

    def __str__(self):
        return str(err_info)


class GeneralClient:
    """通用 thrift RPC client(需要ZK)
    """
    def __init__(self, host=None, port=None,
                 zk_server=None, zk_device=None, server_name=None):
        if host and port:
            self.use_zk = False
            self.host = host
            self.port = port
        else if zk_server and zk_device and server_name:
            self.use_zk = True
            self.zk = ZkMonitor(zk_server, zk_device, server_name)
        else:
            raise ThriftClientInitError()


    def GeneralReq(self, transType, ClientType, reqFuncName, *reqParas):
        '''调用thriftRPC接口函数
        Parameters:
          - transType   <str>: 传输类型
                               F- TFramedTransport
                               B- TBufferedTransport
          - ClientType  <obj>: 服务定义的Clent类
          - reqFuncName <str>: 接口函数名称
          - *reqParas        : 接口函数参数

        Return:
          1: 返回码，0正常，负数异常
          2: CouponTaskInfo 结构list
        '''
        server_host, server_port = self._GetHostInfo()
        if not server_host:
            return -1
        if __DEBUG__:
            print server_host, server_port
        tsocket = None
        transport = None
        rslt = None
        try:
            tsocket = TSocket.TSocket(server_host, server_port)
            tsocket.setTimeout(_DEFAULT_SERVER_TIMEOUT)

            if 'F' == transType or 'f' == transType:
                transport = TTransport.TFramedTransport(tsocket)
            else:
                transport = TTransport.TBufferedTransport(tsocket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = ClientType(protocol)

            transport.open()

            rslt = getattr(client, reqFuncName)(*reqParas)

        except Thrift.TException, tx:
            if __LOG__:
                printlog("Error", "Thrift.TException occurred", tx.message)
            rslt = None

        except Exception, e:
            err_info = traceback.format_exc()
            if __LOG__:
                printlog("Error", "Exception occurred", str(e) + err_info)
            rslt = None

        finally:
            if transport != None:
                transport.close()
            if tsocket != None:
                tsocket.close()
            return rslt


    def _GetHostInfo(self):
        if self.use_zk:
            return self.zk.GetServerInfo()
        else:
            return self.host, self.port
