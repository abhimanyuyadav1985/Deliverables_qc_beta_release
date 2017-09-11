import pickle
import os
import time
import shutil
import datetime
import sys

register_name = 'task_register'
taks_log_name = 'task_log'
task_lock_name = 'task_lock'
status_update_lock_name = 'status_update_lock'

# now entry in register structure = [ command, drive, log_path ]
#this will be used later by the other daemons

def task_execution():
    file_handler = open(os.path.join(os.getcwd(),register_name),'rb')
    task_register = pickle.load(file_handler)
    file_handler.close()
    if len(task_register) == 0:
        print "Task register empty.."
    else:
        for a_entry in task_register:
                print "Now executing : " + a_entry[0]
                cmd = a_entry[0]
                add_to_task_log(a_entry)
                if a_entry[1] == 'segy_qc':
                    create_segy_qc_lock()
                os.system(cmd)
        print "Clearing Register .."
        task_register = []
        os.remove(os.path.join(os.getcwd(), register_name))
        file_handler = open(os.path.join(os.getcwd(), register_name), 'wb')
        pickle.dump(task_register, file_handler)
        file_handler.close()
        print "Done .. "
        print "No unexecuted commands in register.."


def create_segy_qc_lock():
    file_handler = open(os.path.join(os.getcwd(),'segy_qc_lock'),'wb')
    file_handler.write("")
    file_handler.close()
    print "SEGY QC lock created"

def add_to_task_log(a_entry):
    # 1st check if the task log exists
    if os.path.exists(os.path.join(os.getcwd(),taks_log_name)):
        print "Found task log .. "
    else:
        print "Creating a blank task log ..",
        blank_log = []
        file_handler = open(os.path.join(os.getcwd(),taks_log_name),'wb')
        pickle.dump(blank_log,file_handler)
        file_handler.close()
        print "Done .. "
    if os.path.exists(os.path.join(os.getcwd(),status_update_lock_name)):
        time.sleep(10)
        add_to_task_log(a_entry)
    else:
        pass
    manipulate_task_log_lock('create')
    file_handler = open(os.path.join(os.getcwd(), taks_log_name), 'rb')
    task_log_list = pickle.load(file_handler)
    file_handler.close()
    a_entry.append('new')
    task_log_list.append(a_entry)
    os.remove(os.path.join(os.getcwd(),taks_log_name))
    file_handler = open(os.path.join(os.getcwd(), taks_log_name), 'wb')
    pickle.dump(task_log_list,file_handler)
    file_handler.close()
    manipulate_task_log_lock('remove')
    print "Added to task log : " + a_entry[0]


def manipulate_task_log_lock(action):
    if action == 'create':
        print "Locking the task log for reading .. ",
        file_handler = open(os.path.join(os.getcwd(),task_lock_name),'wb')
        file_handler.write("")
        file_handler.close()
        print "Done .. "
    else:
        print "Removing the task log ..",
        os.remove(os.path.join(os.getcwd(),task_lock_name))
        print "Done .. "


def check_task_register():
    if os.path.exists(os.path.join(os.getcwd(),register_name)):
        print "Found task register"
    else:
        sys.exit()


def move_segy_qc_to_segy_qc_buffer():
    src_path = os.path.join(os.getcwd(), 'buffer', register_name)
    dst_path = os.path.join(os.getcwd(), 'buffer', 'register_temp')
    shutil.copy(src_path,dst_path)
    file_handler = open(dst_path,'rb')
    new_cmd = pickle.load(file_handler)
    file_handler.close()
    # Now remove the temp path
    os.remove(dst_path)
    # creare a new SEGY QC buffer if missing
    file_handler = open(os.path.join(os.getcwd(),'buffer','segy_qc_buffer'),'rb')
    current_segy_qc = pickle.load(file_handler)
    file_handler.close()
    for a_cmd in new_cmd:
        if a_cmd[1] == 'segy_qc':
            print "Added to SEGY QC buffer: " +  a_cmd[0]
            current_segy_qc.append(a_cmd)
    os.remove(os.path.join(os.getcwd(),'buffer','segy_qc_buffer'))
    file_handler = open(os.path.join(os.getcwd(), 'buffer', 'segy_qc_buffer'), 'wb')
    pickle.dump(current_segy_qc,file_handler)
    file_handler.close()

def remove_segy_qc_from_buffer_task_register():
    src_path = os.path.join(os.getcwd(), 'buffer', register_name)
    file_handler = open(src_path, 'rb')
    buffer_task_list = pickle.load(file_handler)
    file_handler.close()
    for a_task in buffer_task_list:
        if a_task[1] =='segy_qc':
            print "Reomved from buffer task list: " + a_task[0]
            buffer_task_list.remove(a_task)
    os.remove(src_path)
    file_handler = open(src_path,'wb')
    pickle.dump(buffer_task_list,file_handler)
    file_handler.close()


def append_segy_qc_task_to_buffer_register():
    segy_qc_buffer_path = os.path.join(os.getcwd(), 'buffer', 'segy_qc_buffer')
    task_register_path = os.path.join(os.getcwd(),'buffer',register_name)
    file_handler = open(segy_qc_buffer_path,'rb')
    segy_qc_buffer_list = pickle.load(file_handler)
    file_handler.close()
    # first check if segy qc lock is present or not
    segy_qc_lock_path = os.path.join(os.getcwd(),'segy_qc_lock')
    if os.path.exists(segy_qc_lock_path):
        print "A SEGY QC job is already running .. omitting new job addition.."
    else:
        if len(segy_qc_buffer_list) != 0:
            cmd_to_add = segy_qc_buffer_list[0]
            segy_qc_buffer_list.remove(cmd_to_add)
            check_and_create_buffer_task_register()
            file_handler = open(task_register_path,'rb')
            print "Adding to task buffer : " + cmd_to_add[0]
            buffer_task_log = pickle.load(file_handler)
            file_handler.close()
            buffer_task_log.append(cmd_to_add)
            os.remove(task_register_path)
            file_handler = open(task_register_path,'wb')
            pickle.dump(buffer_task_log,file_handler)
            file_handler.close()
            print "Moved from SEGY QC buffer : " + cmd_to_add[0]
            os.remove(segy_qc_buffer_path)
            file_handler = open(segy_qc_buffer_path,'wb')
            pickle.dump(segy_qc_buffer_list,file_handler)
            file_handler.close()
        else:
            print "No new SEGY QC task to move to buffer_task_registe"

def check_and_create_buffer_task_register():
    if os.path.exists(os.path.join(os.getcwd(), 'buffer', register_name)):
        pass
    else:
        file_handler = open(os.path.join(os.getcwd(), 'buffer', register_name),'wb')
        empty_list = []
        pickle.dump(empty_list,file_handler)
        file_handler.close()


def task_execution_lock(status):
    lock_path = os.path.join(os.getcwd(),'buffer','task_daemon_lock')
    if status == 'create':
        print "Creating Task execution deamon lock on buffer .."
        file_handler = open(lock_path,'wb')
        file_handler.write("")
        file_handler.close()
    elif status == 'remove':
        print "Task execution daemon lock on buffer removed .. "
        os.remove(lock_path)

def check_and_create_segy_buffer():
    if os.path.exists(os.path.join(os.getcwd(),'buffer','segy_qc_buffer')):
        pass
    else:
        print "SEGY QC buffer missing creating new .."
        file_handler = open(os.path.join(os.getcwd(), 'buffer', 'segy_qc_buffer'), 'wb')
        empty_list = []
        pickle.dump(empty_list,file_handler)
        file_handler.close()

def main():
    while True:
        print str(datetime.datetime.now())
        check_and_create_segy_buffer()
        if os.path.exists(os.path.join(os.getcwd(), 'buffer', 'buffer_lock')):
            print "Buffer lock enabled omitting buffer transfer"
        else:
            if os.path.exists(os.path.join(os.getcwd(), 'buffer', register_name)):
                task_execution_lock('create')
                move_segy_qc_to_segy_qc_buffer()
                remove_segy_qc_from_buffer_task_register()
                append_segy_qc_task_to_buffer_register()
                print "Removing the old register ..",
                os.remove(os.path.join(os.getcwd(), register_name))
                print "Done .."
                print "Now creating new register from buffer ..",
                src_path = os.path.join(os.getcwd(), 'buffer', register_name)
                dst_path = os.getcwd()
                shutil.move(src=src_path, dst=dst_path)
                task_execution_lock('remove')
                print "Done .. "
            else:
                append_segy_qc_task_to_buffer_register()
                print "Buffer empty.."
        check_task_register()
        task_execution()
        time.sleep(30)

if __name__ == "__main__":
    main()

