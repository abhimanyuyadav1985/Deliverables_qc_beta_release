from configuration import *
import os
import pickle
import posixpath
import shutil
import cStringIO

file_path = os.path.join(os.getcwd(), conn_config_file)

def main():
    print "You are in DUG ops script"

def transfer_task_logger(DUG_connection_obj):
    print "Now transferring task_logger",
    file_path_2 = '/d/home/share/bin/task_logger.py'
    local_path_2 = os.path.join(os.getcwd(), "dug_ops", 'task_logger.py')
    DUG_connection_obj.sftp_client.put(local_path_2, file_path_2)
    print "done.."

def transfer_run_log_fetcher(DUG_connection_obj):
    print "Now transferring the run log fetcher",
    file_path_2 = '/d/home/share/bin/run_log_fetcher.py'
    local_path_2 = os.path.join(os.getcwd(), "dug_ops", 'run_log_fetcher.py')
    DUG_connection_obj.sftp_client.put(local_path_2, file_path_2)
    print "done.."

def transfer_directory_checking_script(sftp_client):
    file_path_2 = '/d/home/share/bin/directory_checking_script.py'
    local_path_2 = os.path.join(os.getcwd(), "dug_ops", 'directory_checking_script.py')
    print "Transferring the directory checking script .... ",
    sftp_client.put(local_path_2, file_path_2)
    print "done.."

def check_generic_path(DUG_connection_obj,path_to_check):
    cmd = "python " + '/d/home/share/bin/directory_checking_script.py ' + path_to_check
    stdin, stdout, stderr = DUG_connection_obj.ws_client.exec_command(cmd)
    outlines = stdout.readlines()
    status = outlines[0].rstrip()
    #print "The status from generic path check ==" + status
    return status

def transfer_SEGD_QC_parser_script(DUG_connection_obj):
    path_to_check = '/d/home/share/bin/SEGD_QC_parser.py'
    status = check_generic_path(DUG_connection_obj, path_to_check)
    print status
    #if status == "False":
    file_path_2 = '/d/home/share/bin/SEGD_QC_parser.py'
    local_path_2 = os.path.join(os.getcwd(), "dug_ops", 'SEGD_QC_parser.py')
    print "Transferring the SEGD tape QC parsing script .... ",
    DUG_connection_obj.sftp_client.put(local_path_2, file_path_2)
    print "done.."

def transfer_base_64_encoder(DUG_connection_obj):
    path_to_check = '/d/home/share/bin/base_64_encoder.py'
    status = check_generic_path(DUG_connection_obj, path_to_check)
    print status
    #if status == "False":
    file_path_2 = '/d/home/share/bin/base_64_encoder.py'
    local_path_2 = os.path.join(os.getcwd(), "dug_ops", 'base_64_encoder.py')
    print "Transferring the base 64 encoder script .... ",
    DUG_connection_obj.sftp_client.put(local_path_2, file_path_2)
    print "done.."

def transfer_SEGY_check_script(DUG_connection_obj):
    dug_path = str(DUG_connection_obj.DUG_proj_path)
    project_name = str(DUG_connection_obj.DUG_project_name)
    path_to_check = dug_path + '/segy-hdr-chk-mp.tar.gz'

    status = check_generic_path(DUG_connection_obj,path_to_check)
    print status
    if status == "False":
        file_path_2 = dug_path + '/segy-hdr-chk-mp.tar.gz'
        local_path_2 = os.path.join(os.getcwd(), "dug_ops", 'segy-hdr-chk-mp.tar.gz')
        print "Transferring the SEGY checking scripts..... ",
        DUG_connection_obj.sftp_client.put(local_path_2, file_path_2)
        print "done.."
    print "Checking if it has been decompressed before and softlink created..",
    path_to_check = '/d/home/share/bin/' + project_name + "_segy-hcm"
    status = check_generic_path(DUG_connection_obj,path_to_check)
    print status
    if status == "False":
        print "Now decompressing the tarball and creating the softlink..",
        decompress_and_softlink_creation(DUG_connection_obj,path_to_check)

def transfer_run_daemons(DUG_connection_obj):
    transfer_task_execution_daemon(DUG_connection_obj)

def create_register_paths(DUG_connection_obj):
    dug_path = str(DUG_connection_obj.DUG_proj_path)
    path_list = [posixpath.join(dug_path,'register'), posixpath.join(dug_path,'register','buffer')]
    for a_path in path_list:
        status = check_generic_path(DUG_connection_obj,a_path)
        if status =='False':
            print "Creating : " + a_path
            create_generic_directory(DUG_connection_obj,a_path)
        else:
            print 'Found : ' + a_path

def transfer_task_execution_daemon(DUG_connection_obj):
    dug_path = str(DUG_connection_obj.DUG_proj_path)
    path_to_check = posixpath.join(dug_path,'register','deliverables_qc_task_execution_daemon.py')
    status = check_generic_path(DUG_connection_obj, path_to_check)
    print status
    if status == "False":
        file_path = path_to_check
        local_path = os.path.join(os.getcwd(),'dug_ops','deliverables_qc_task_execution_daemon.py')
        print "Transferring the Deliverablaes QC task execution Daemon..... ",
        DUG_connection_obj.sftp_client.put(local_path, file_path)
        print "done.."
    else:
        print "Transferring the Deliverablaes QC task execution Daemon..... "

def transfer_lock_states_and_registers(DUG_connection_obj):
    create_register_paths(DUG_connection_obj)
    transfer_task_register(DUG_connection_obj)


def transfer_task_register(DUG_connection_obj):
    dug_path = str(DUG_connection_obj.DUG_proj_path)
    path_to_check = posixpath.join(dug_path, 'register', 'task_register')
    status = check_generic_path(DUG_connection_obj, path_to_check)
    print status
    if status == "False":
        file_path = path_to_check
        local_path = os.path.join(os.getcwd(), 'dug_ops', 'task_register')
        if os.path.exists(local_path):
            print "Transferring the empty SEGD QC register..... ",
            DUG_connection_obj.sftp_client.put(local_path, file_path)
            print "done.."
        else:
            print "SEGD QC Register lock absent on local machine"
    else:
        print "SEGD QC Register already exists on remote host.. "

def decompress_and_softlink_creation(DUG_connection_obj,path_to_check):
    DUG_segy_path = str(DUG_connection_obj.DUG_proj_path)
    cmd = "tar -xf " + DUG_segy_path + "/segy-hdr-chk-mp.tar.gz -C " + DUG_segy_path + "/"
    DUG_connection_obj.ws_client.exec_command(cmd)
    print "decompression done..",
    cmd = "ln -s " + DUG_segy_path + '/segy-hdr-chk-mp/segy-hdr-chk-mp ' + path_to_check
    DUG_connection_obj.ws_client.exec_command(cmd)
    print "softlink created .."


def create_generic_directory(DUG_connection_obj,path_to_create):
    cmd = "mkdir " + path_to_create
    stdin, stdout, stderr = DUG_connection_obj.ws_client.exec_command(cmd)
    if  stdout.readlines()==[] and stderr.readlines() == []:
        return True
    else:
        return False

def run_command_on_tape_server(DUG_connection_obj,cmd,dev_to_use):
    #now 1st check the busy device file
    path_dev_file = os.path.join(os.getcwd(),'temp','busy_dev')
    if os.path.exists(path_dev_file):
        file_handler = open(path_dev_file,'rb')
        busy_dev_list = pickle.load(file_handler)
        file_handler.close()
    else:
        busy_dev_list = []
    if dev_to_use in busy_dev_list:
        print "Aborting Busy: " + dev_to_use
        return None
    else:
        print "Executing : " + cmd
        stdin, stdout, stderr = DUG_connection_obj.ts_client.exec_command(cmd)
        print "done .. "
        return stdout

def run_command_on_ws_client(DUG_connection_obj,cmd):
    stdin, stdout, stderr = DUG_connection_obj.ws_client.exec_command(cmd)


def fetch_directory_content_list(DUG_connection_obj,cmd):
    stdin, stdout, stderr = DUG_connection_obj.ws_client.exec_command(cmd)
    directory_listing = []
    for item in stdout.readlines():
        directory_listing.append(item.rstrip())
    return directory_listing


def get_SEGD_QC_status(DUG_conenction_obj, path):
    cmd = str('python /d/home/share/bin/SEGD_QC_parser.py ' + path )
    stdin, stdout, stderr = DUG_conenction_obj.ws_client.exec_command(cmd)
    std_out = []
    for line in stdout.readlines():
        std_out.append(line)
    if int(std_out[0][1]) == 1:
        return True
    else:
        return False


def get_SEGD_QC_run_finish_status(DUG_conenction_obj, path):
    cmd = str('python /d/home/share/bin/SEGD_QC_parser.py ' + path)
    stdin, stdout, stderr = DUG_conenction_obj.ws_client.exec_command(cmd)
    std_out = []
    for line in stdout.readlines():
        std_out.append(line)
    print std_out[0]
    if int(std_out[0][0]) == 1:
        return True
    else:
        return False


def SFTP_generic_file(DUG_connection_obj,local_path,remote_path):
    DUG_connection_obj.sftp_client.put(local_path, remote_path)

def return_encoded_log(DUG_connection_obj,log_path):
    cmd = str('python /d/home/share/bin/base_64_encoder.py ' + log_path)
    stdin, stdout, stderr = DUG_connection_obj.ws_client.exec_command(cmd)
    return stdout.read()

def get_file_timestamp(DUG_connection_obj,file_path):
    cmd = str('stat ' + file_path)
    stdin, stdout, stderr = DUG_connection_obj.ws_client.exec_command(cmd)
    std_out = []
    for line in stdout.readlines():
        std_out.append(line)
    return std_out


def append_register_entry(DUG_connection_obj, register_obj):
    # 1st read the execution log state
    local_path = os.path.join(os.getcwd(), 'dug_ops', 'buffer_lock')
    dug_path = DUG_connection_obj.DUG_proj_path
    remote_path = posixpath.join(dug_path, 'register', 'buffer', 'buffer_lock')
    print "Buffer lock created on remote host.."
    DUG_connection_obj.sftp_client.put(local_path, remote_path)
    file_handler = open(os.path.join(os.getcwd(), 'temp', 'task_register'), 'rb')
    segd_qc_register = pickle.load(file_handler)
    file_handler.close()
    segd_qc_register.append(register_obj)
    print "Adding the command to local buffer",
    os.remove(os.path.join(os.getcwd(), 'temp', 'task_register'))
    file_handler = open(os.path.join(os.getcwd(), 'temp', 'task_register'), 'wb')
    pickle.dump(segd_qc_register, file_handler)
    file_handler.close()
    print "Done .."
    remote_register_path = posixpath.join(dug_path, 'register', 'buffer', 'task_register')
    local_register_path = os.path.join(os.getcwd(), 'temp', 'task_register')
    status = check_generic_path(DUG_connection_obj, remote_register_path)
    if status == 'True':
        DUG_connection_obj.sftp_client.remove(remote_register_path)
    print "Transferring buffer to remote host..",
    DUG_connection_obj.sftp_client.put(local_register_path, remote_register_path)
    print "Done .."
    print "Flusing local buffers ..",
    shutil.copy(os.path.join(os.getcwd(), 'dug_ops', 'task_register'), os.path.join(os.getcwd(), 'temp'))
    print "Done"
    print "Now deleting buffer lock .."
    DUG_connection_obj.sftp_client.remove(remote_path)
    print "Done.."


if __name__ == '__main__':
    main()