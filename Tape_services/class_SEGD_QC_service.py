from configuration import *
from dug_ops.DUG_ops import fetch_directory_content_list
from database_engine.DB_ops import get_list_of_applicable_SEGD_tapes,get_min_max_ffid_tuple_for_tape
import posixpath
from dug_ops.DUG_ops import append_register_entry

class SEGD_QC_service(object):
    def __init__(self,parent):
        self.parent = parent
        self.name = "SEGD_QC_service"

    def run(self):
        run_cmd = str("nohup " + segd_qc_script + " tape=/dev/" + str(self.parent.tape_drive)+ " disk=" + str(self.SEGD_path) + " log=" + str(self.logfile) + " firstdisk=" + str(self.min_ffid) + " lastdisk=" + str(self.max_ffid) + ' > /dev/null 2>&1 &')
        if use_mode == 'Demo':
            print "Running in DEMO mode command will only be printed not executed ...."
        elif use_mode == 'Production':
            print run_cmd
            self.check_for_previous_run()
            segd_qc_register_obj = [run_cmd , self.parent.tape_drive, self.logfile]
            print "Now adding the SEGD QC task to buffers .."
            append_register_entry(self.parent.DUG_connection_obj,segd_qc_register_obj)

    def check_for_previous_run(self):
        result = self.parent.db_connection_obj.sess.query(self.parent.db_connection_obj.SEGD_qc).filter(
            self.parent.db_connection_obj.SEGD_qc.log_path == self.logfile).first()
        if result == None:
            pass
        else:
            print "Previous run deltected .. deleting it from database for integrity..",
            self.parent.db_connection_obj.sess.delete(result)
            self.parent.db_connection_obj.sess.commit()
            print "done .. "


    def set_SEGD_path(self,Seq_dir):
        self.SEGD_path = posixpath.join(self.parent.dir_service.data_dir_path_dict['segd.segd'],Seq_dir)
        print "The SEGD data directory set:: " + self.SEGD_path

    def set_logfile(self):
        self.logfile = self.parent.file_service.set_segd_log_file_path()
        print "The log file set :: " + self.logfile

    def get_list_of_available_segd_seq(self):
        dir_path = self.parent.dir_service.data_dir_path_dict['segd.segd']
        cmd = str("ls " + dir_path)
        dir_list_for_combo = fetch_directory_content_list(self.parent.DUG_connection_obj,cmd)
        return dir_list_for_combo

    def get_list_of_applicable_segd_tapes(self):
        tape_list = get_list_of_applicable_SEGD_tapes(self.parent.db_connection_obj,self.parent.seq_name)
        return tape_list

    def set_min_max_ffid(self):
        (self.min_ffid,self.max_ffid) = get_min_max_ffid_tuple_for_tape(self.parent.db_connection_obj,self.parent.tape_name)


if __name__ == '__main__':
    test = SEGD_QC_service()

