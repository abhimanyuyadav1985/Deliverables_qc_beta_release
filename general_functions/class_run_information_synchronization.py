import posixpath
from datetime import datetime
from PyQt4 import QtCore
import time
from dug_ops.DUG_ops import check_generic_path
import os
import pickle
from configuration import conn_config_file
import paramiko

class run_information_sync(QtCore.QObject):

    doingWork = QtCore.pyqtSignal(bool, str, str)
    cmdcontrol = QtCore.pyqtSignal(str)
    update_tape_dashboard = QtCore.pyqtSignal()

    def __init__(self,dug_proj_path,thread_name):
        super(run_information_sync,self).__init__()
        self.thread_name = thread_name
        self.DUG_proj_path = dug_proj_path
        self.DUG_connection_obj = thread_DUG_client()


    def run_and_flush(self):
        while True:
            dug_proj_path = self.DUG_proj_path
            register_dir_path = posixpath.join(dug_proj_path,'register')
            cmd = str('python /d/home/share/bin/task_logger.py ' + register_dir_path + ' flush')
            self.cmdcontrol.emit(cmd)
            string_to_add = str(str(datetime.now()) + ' : Refreshed the task log status')
            print string_to_add
            self.doingWork.emit(True,string_to_add,self.thread_name)
            self.create_busy_device_list()
            self.update_tape_dashboard.emit()
            time.sleep(30)


    def create_busy_device_list(self):
        self.import_task_log()
        busy_dev_path = os.path.join(os.getcwd(),'temp','busy_dev')
        busy_dev_list = []
        for a_task in self.task_list:
            busy_dev_list.append(a_task[1])
        if os.path.exists(busy_dev_path):
            os.remove(busy_dev_path)
        else:
            print "Creating busy devices file : " + busy_dev_path
            file_handler = open(busy_dev_path, 'wb')
            pickle.dump(busy_dev_list, file_handler)
            file_handler.close()
            print "Done ..."


    def import_task_log(self):
        proj_path = self.DUG_proj_path
        remote_path = posixpath.join(proj_path, 'register', 'task_log')
        local_path = os.path.join(os.getcwd(), 'temp', 'task_log')
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except:
                print "Exception: Local task_log_busy"
        status = check_generic_path(self.DUG_connection_obj, remote_path)
        if status == 'True':
            # Now FTP the file
            print "Now copying over the task log from remote host ..",
            try:
                self.DUG_connection_obj.sftp_client.get(remote_path, local_path)
                print 'Done ..'
                self.local_path = local_path
                self.extract_task_info()
            except:
                print "Exception: Unable to copy to local host "
        else:
            print "Task log missing on remote host.."


    def extract_task_info(self):
        file_handler = open(self.local_path, 'rb')
        self.task_list = pickle.load(file_handler)
        file_handler.close()
        if len(self.task_list) == 0:
            print "No active tasks available"



class thread_DUG_client(object):
    def __init__(self):
        file_path = os.path.join(os.getcwd(),
                                 conn_config_file)  # use this string for production mode in the application

        file_handler = open(file_path, 'rb')
        obj_config = pickle.load(file_handler)
        file_handler.close()
        host = obj_config.DUG_IP
        port = 22
        DUG_user = obj_config.DUG_user
        DUG_pword = obj_config.DUG_pword
        self.transport = paramiko.Transport((host, port))
        self.transport.connect(username=DUG_user, password=DUG_pword)
        print "Transport for DUG connection now setup.."
        self.ws_client = paramiko.SSHClient()
        self.ws_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ws_client.connect(host, username=DUG_user, password=DUG_pword)
        print "The DUG WS client is now active .."
        self.sftp_client = paramiko.SFTPClient.from_transport(self.transport)
        print "The DUG sftp client is now active .. "





