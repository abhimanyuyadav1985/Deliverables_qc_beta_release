from PyQt4 import QtGui,QtCore
import posixpath
import os
from dug_ops.DUG_ops import check_generic_path
import pickle
from general_functions.general_functions import create_central_labels


class running_task_log(QtGui.QWidget):
    def __init__(self, parent):
        super(running_task_log, self).__init__()
        self.parent = parent
        self.DUG_connection_obj = self.parent.DUG_connection_obj
        self.grid = QtGui.QGridLayout()
        self.setLayout(self.grid)

        self.setWindowTitle("Active Task information")

        self.import_task_log()
        self.extract_task_info()

        if len(self.task_list) == 0:
            print "No active tasks available"
            self.add_none()
        else:
            self.add_info_widgets()


    def import_task_log(self):
        proj_path = self.DUG_connection_obj.DUG_proj_path
        remote_path = posixpath.join(proj_path,'register','task_log')
        local_path = os.path.join(os.getcwd(),'temp','task_log')
        if os.path.exists(local_path):
            os.remove(local_path)
        status = check_generic_path(self.DUG_connection_obj,remote_path)
        if status == 'True':
            #Now FTP the file
            print "Now copying over the task log from remote host ..",
            self.DUG_connection_obj.sftp_client.get(remote_path,local_path)
            print 'Done ..'
            self.local_path = local_path
        else:
            print "Task log missing on remotre host.."

    def add_none(self):
        message = "No active tasks"
        self.grid.addWidget(create_central_labels(message),0,0)

    def extract_task_info(self):
        file_handler = open(self.local_path,'rb')
        self.task_list = pickle.load(file_handler)
        file_handler.close()

    def add_info_widgets(self):
        self.tabs = QtGui.QTabWidget()
        self.tabs.setMinimumWidth(1200)
        self.grid.addWidget(self.tabs,0,0,1,5)
        pb_refresh = QtGui.QPushButton('Refresh logs')
        pb_refresh.clicked.connect(self.refresh_logs)
        self.grid.addWidget(pb_refresh,1,4)
        self.tab_list = []
        for a_task in self.task_list:
            self.tab_list.append(single_task_information(self,str(a_task[2])))
            self.tabs.addTab(single_task_information(self,str(a_task[2])),str(str(a_task[1])))

    def refresh_logs(self):
        print "Tool will be available soon.."

class single_task_information(QtGui.QScrollArea):
    def __init__(self,parent,log_path):
        super(single_task_information, self).__init__()
        self.parent = parent
        self.setMinimumWidth(1200)
        self.DUG_connection_obj = self.parent.DUG_connection_obj
        self.textbox = QtGui.QTextEdit()
        self.textbox.setStyleSheet('''
            QTextEdit {
                font: 10pt "Consolas";
            }
        ''')
        self.textbox.setMinimumWidth(1200)
        self.textbox.setMinimumHeight(800)
        self.log_path = log_path
        self.setWidget(self.textbox)
        self.update_info()


    def update_info(self):
        cmd = 'python /d/home/share/bin/run_log_fetcher.py ' + self.log_path
        stdin, stdout, stderr = self.DUG_connection_obj.ws_client.exec_command(cmd)
        encoded_text = stdout.read()
        decoded_text = encoded_text.decode('base64')
        for a_line in decoded_text.split("\n"):
            self.textbox.append(a_line.rstrip("\n"))






