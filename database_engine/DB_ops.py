import psycopg2
# -*- coding: utf-8 -*-
import pickle
from configuration import *
from unipath import Path
import sys
from sqlalchemy import or_,and_
#------------------------------------------------------------------------------------------------------------
#Function to check the connection to the desired database_engine in from configuration setup
#------------------------------------------------------------------------------------------------------------
def test_DB_connection(curr):
    print "Testing the DB connection now ......"
    try:
        curr.execute("SELECT VERSION()")
        results = curr.fetchone()
        ver = results[0]
        if (ver is None):
            print "Please check the details once again as we cannot find Postgres vversion "
            return False
        else:
            print ver
            return
    except:
        print "ERROR IN CONNECTION, please try again"
        return False

def test_db_connection_for_config(obj):
    db_name = str(obj.db_name.text())
    db_user = str(obj.db_user.text())
    db_pword = str(obj.db_pword.text())
    db_port = str(obj.db_port.text())

    host_IP = str(obj.host_IP.text())
    host_user = str(obj.host_user.text())
    host_pword = str(obj.host_pword.text())
    try:
        conn = psycopg2.connect(database=db_name, user=db_user, password=db_pword, port=1111)
        curr = conn.cursor()
        test_DB_connection(curr)
        curr.close()
        print "Connection to database_engine successful !!!!!!!!!!!!!!!"
    except:
        print "Unable to connect to the Server, Does the tunnel from database server to local port 1111 exist ????"
        print "Exiting application now"
        sys.exit()

def fetch_config_info():
    file_path = os.path.join(os.getcwd(), conn_config_file)
    print file_path
    file_handler = open(file_path, "rb")
    obj_config = pickle.load(file_handler)
    file_handler.close()
    db_name = str(obj_config.db_name)
    db_user = str(obj_config.db_user)
    host_IP = str(obj_config.host_IP)
    DUG_host = obj_config.DUG_IP
    DUG_user = obj_config.DUG_user
    dug_path = obj_config.DUG_segy_path
    return [host_IP, db_user, db_name, DUG_host, DUG_user, dug_path]

#-----------------------------------------------------------------------------------------------------------
# Get the project information dictionary
#-----------------------------------------------------------------------------------------------------------
def fetch_project_info(obj):
    search_result = obj.sess.query(obj.Project_info).order_by(obj.Project_info.ip).all()
    return search_result


#-------------------------------------------------------------------------------------------------------------
# Deliverables functions
#--------------------------------------------------------------------------------------------------------------
def fetch_deliverables_list(obj):
    search_result  = obj.sess.query(obj.Deliverables).order_by(obj.Deliverables.id).all()
    search_list = []
    for deliliverable_item in search_result:
        search_list.append(deliliverable_item.__dict__)
    return search_list

def fetch_shipments_list(obj):
    search_result = obj.sess.query(obj.Shipments).order_by(obj.Shipments.id).all()
    search_list = []
    for shipment_item in search_result:
        search_list.append(shipment_item.__dict__)
    return search_list

def add_deliverable(obj, dao_object):
    obj.sess.add(dao_object)
    obj.sess.commit()

def add_shipment(obj,dao_object):
    obj.sess.add(dao_object)
    obj.sess.commit()

def fetch_single_deliverable(obj,id):
    search_result = obj.sess.query(obj.Deliverables).filter(obj.Deliverables.id == id).all()
    return search_result[0].__dict__

def delete_deliverable_obj(obj,id):

    deliverable_to_delete = obj.sess.query(obj.Deliverables).filter(obj.Deliverables.id == id)
    #print deliverable_to_delete
    deliverable_to_delete.delete()
    obj.sess.commit()

def delete_shipment_obj(obj,id):
    shipment_to_delete = obj.sess.query(obj.Shipments).filter(obj.Shipments.id == id)
    # print deliverable_to_delete
    shipment_to_delete.delete()
    obj.sess.commit()


def add_deliverable_data_dir(obj,dao_object):
    #sess.query(obj.Deliverables_data_dir).filter(obj.Deliverables_data_dir.path == dao_object.path).count()
    if obj.sess.query(obj.Deliverables_data_dir).filter(obj.Deliverables_data_dir.path == dao_object.path).count() >0:
        print dao_object.path + "  exists in database .."
        return False
    else:
        print "Now adding to Db::" + dao_object.path
        obj.sess.add(dao_object)
        obj.sess.commit()
        return False

def add_deliverable_qc_dir(obj,dao_object):
    #print sess.query(obj.Deliverables_qc_dir).filter(obj.Deliverables_qc_dir.path == dao_object.path).count()
    if obj.sess.query(obj.Deliverables_qc_dir).filter(obj.Deliverables_qc_dir.path == dao_object.path).count() > 0:
        print dao_object.path + "  exists in database .."
        return False
    else:
        print "Now adding to db:: " + dao_object.path
        obj.sess.add(dao_object)
        obj.sess.commit()
        return False

def verify_if_deliverable_id_is_correct(obj,id):
    deliverable_list = obj.sess.query(obj.Deliverables).order_by(obj.Deliverables.id).all()
    id_list = []
    for deliverable in deliverable_list:
        id_list.append(str(deliverable.id))
    if id in id_list:
        return True
    else:
        return False

def get_deliverable_object(obj,id):
    deliverable =  obj.sess.query(obj.Deliverables).filter(obj.Deliverables.id == id).first()
    return deliverable


def check_and_add_raw_seq_info(obj):
    cmd = """
        SELECT l.sequence_number AS seq,
            l.real_line_name,
            l.preplot_name,
            a.first_ffid,
            a.first_shot,
            orca.ffid(l.sequence_number, e.fgsp) AS fg_ffid,
            e.fgsp,
            orca.ffid(l.sequence_number, e.lgsp) AS lg_ffid,
            e.lgsp,
            a.last_ffid,
            a.last_shot,
            to_char(l.start_time, 'DD-MM-YYYY'::text) AS start_data
           FROM orca.line l
             JOIN orca.edit_good_shot_point e USING (sequence_number)
             JOIN orca.all_sp a USING (sequence_number)
          ORDER BY l.sequence_number;
          """
    s = obj.scoped_session()
    result = s.execute(cmd)
    s.close()
    print "Now synchronizing the Raw seq info for orca data "

    for seq_data in result:
        #print seq_data
        new_entry = obj.Raw_seq_info()
        (new_entry.seq, new_entry.real_line_name, new_entry.preplot_name, new_entry.first_ffid, new_entry.first_shot,new_entry.fg_ffid, new_entry.fgsp, new_entry.lg_ffid, new_entry.lgsp, new_entry.last_ffid, new_entry.last_shot,new_entry.start_data) = seq_data
        old_object = obj.sess.query(obj.Raw_seq_info).filter(obj.Raw_seq_info.seq == new_entry.seq).first()
        if old_object != None:
            #print "Found an existing object ... "
            for a_key in new_entry.__dict__.keys():
                if a_key == '_sa_instance_state':
                    pass
                else:
                    if str(new_entry.__dict__[a_key]) == str(old_object.__dict__[a_key]):
                        pass
                    else:
                        print "Update => " +str(new_entry.seq) + " : " + str(a_key) + " old: " + str(
                            old_object.__dict__[a_key]) + " new: " + str(new_entry.__dict__[a_key])
                        if new_entry.__dict__[a_key] != None:
                            setattr(old_object, a_key, str(new_entry.__dict__[a_key]))

                        else:
                            print "Omit as the new entry is None"

        else:
               print "creating the new entry for " + str(new_entry.seq) + " ",
               obj.sess.add(new_entry)
               print "done..."
    obj.sess.commit()

def get_deliverables_dao_objects(obj):
    search_result = obj.sess.query(obj.Deliverables).order_by(obj.Deliverables.id).all()
    return search_result

def get_segd_qc_path(obj,id,set_no):
    search_obj = obj.sess.query(obj.Deliverables_qc_dir).filter(obj.Deliverables_qc_dir.deliverable_id == id).filter(obj.Deliverables_qc_dir.set_number == set_no).first()
    return search_obj

def get_list_of_applicable_SEGD_tapes(obj, seq_name):
    seq_from_raw_seq_info = obj.sess.query(obj.Raw_seq_info).filter(obj.Raw_seq_info.real_line_name == seq_name).first()
    seq_id = seq_from_raw_seq_info.seq
    tapes = obj.sess.query(obj.SEGD_tapes).filter(obj.SEGD_tapes.sequence_number == seq_id).all()
    tape_list = []
    for obj in tapes:
        tape_list.append(obj.name)
    return tape_list

def get_min_max_ffid_tuple_for_tape(obj,tape_name):
    entry = obj.sess.query(obj.SEGD_tapes).filter(obj.SEGD_tapes.name == tape_name).first()
    min_ffid = entry.first_ffid
    max_ffid = entry.last_ffid
    return (min_ffid,max_ffid)

def get_list_of_segd_deliverables(obj):
    deliverables_list = obj.sess.query(obj.Deliverables).filter(obj.Deliverables.class_d == 'SEGD').order_by(obj.Deliverables.id).all()
    return deliverables_list

def get_all_available_segd_tapes_in_orca(obj):
    tape_obj_list = obj.sess.query(obj.SEGD_tapes).order_by(obj.SEGD_tapes.name).all()
    return tape_obj_list

def check_previous_passed_for_SEGD_qc(obj,id,set_no):
    result = obj.sess.query(obj.SEGD_qc).filter(obj.SEGD_qc.qc_status == True, obj.SEGD_qc.deliverable_id == id,obj.SEGD_qc.set_no == set_no).order_by(obj.SEGD_qc.tape_no).all()
    return result

def check_previous_run_for_SEGD_qc(obj,id,set_no):
    result = obj.sess.query(obj.SEGD_qc).filter(obj.SEGD_qc.run_status == True, obj.SEGD_qc.deliverable_id == id,
                                                obj.SEGD_qc.set_no == set_no).order_by(obj.SEGD_qc.tape_no).all()
    return result

def fetch_seq_id_from_name(obj,seq_name):
    seq_from_raw_seq_info = obj.sess.query(obj.Raw_seq_info).filter(obj.Raw_seq_info.real_line_name == seq_name).first()
    seq_id = seq_from_raw_seq_info.seq
    return seq_id

def add_SEGD_QC_obj(obj,dao_obj):
    print "Now adding to SEGD QC DB Table: " + str(dao_obj.deliverable_id)+ " Set no: " + str(dao_obj.set_no) + " Tape no: " + dao_obj.tape_no
    obj.sess.add(dao_obj)
    obj.sess.commit()

def get_all_production_sequences(obj):
    prod_seq_list = obj.sess.query(obj.Raw_seq_info).order_by(obj.Raw_seq_info.seq).all()
    return prod_seq_list

def fetch_seq_name_from_id(obj,seq_id):
    seq_from_raw_seq_info = obj.sess.query(obj.Raw_seq_info).filter(obj.Raw_seq_info.seq == seq_id).first()
    if seq_from_raw_seq_info is not None:
        seq_name = seq_from_raw_seq_info.real_line_name
    else:
        seq_name = "NTBP"
    return seq_name

def get_all_SEGD_qc_entries(obj):
    segd_qc_list = obj.sess.query(obj.SEGD_qc).order_by(obj.SEGD_qc.tape_no).all()
    return segd_qc_list

def get_project_name(obj):
    project_info = obj.sess.query(obj.Project_info).order_by(obj.Project_info.ip).first()
    project_name = project_info.project
    return project_name

def get_list_of_SEGY_deliverables(obj):
    deliverables_list = obj.sess.query(obj.Deliverables).filter(obj.Deliverables.class_d == 'SEGY').order_by(obj.Deliverables.id).all()
    return deliverables_list

def get_list_of_seq_SEGY_deliverables(obj):
    deliverables_list = obj.sess.query(obj.Deliverables).filter(obj.Deliverables.class_d == 'SEGY').filter(obj.Deliverables.type == 'SEQG').order_by(obj.Deliverables.id).all()
    return deliverables_list

def get_SEGD_QC_object_list_for_deliverable_set(obj,deliverable_id,set_no):
    obj_list = obj.sess.query(obj.SEGD_qc).filter(obj.SEGD_qc.deliverable_id == deliverable_id ).filter(obj.SEGD_qc.set_no == set_no).order_by(obj.SEGD_qc.seq_id).all()
    return obj_list

def check_and_add_media_list_obj(obj,media_obj):
    status = obj.sess.query(obj.Media_list).filter(obj.Media_list.deliverable_id == media_obj.deliverable_id).filter(obj.Media_list.set_no == media_obj.set_no).filter(obj.Media_list.media_label == media_obj.media_label).first()
    if status == None:
        obj.sess.add(media_obj)
        obj.sess.commit()
    else:
        pass

def fetch_shipment_objects_list(obj):
    obj_list = obj.sess.query(obj.Shipments).order_by(obj.Shipments.id).all()
    return obj_list

def fetch_deliverable_objects_list(obj):
    obj_list = obj.sess.query(obj.Deliverables).order_by(obj.Deliverables.id).all()
    return obj_list

def fetch_media_list_did_set(obj,deliverable_id,set_no):
    obj_list = obj.sess.query(obj.Media_list).filter(obj.Media_list.deliverable_id ==  deliverable_id).filter(obj.Media_list.set_no == set_no).all()
    return obj_list

def add_usb_list_obj(obj, new_usb_label):
    obj.sess.add(new_usb_label)
    obj.sess.commit()

def check_usb_list_obj(obj, new_usb_name):
    result = obj.sess.query(obj.USB_list).filter(obj.USB_list.label == new_usb_name).first()
    return result

def fetch_usb_list_dict(obj):
    search_result = obj.sess.query(obj.USB_list).order_by(obj.USB_list.usb_id).all()
    search_list = []
    for item in search_result:
        search_list.append(item.__dict__)
    return search_list

def delete_usb_list_obj(obj,id):
    obj_to_delete = obj.sess.query(obj.USB_list).filter(obj.USB_list.usb_id == id)
    #print deliverable_to_delete
    obj_to_delete.delete()
    obj.sess.commit()

def get_all_available_usb(obj):
    result = obj.sess.query(obj.USB_list).order_by(obj.USB_list.usb_id).all()
    return result

def get_all_SEGY_deliverable_for_tape_write(obj):
    result = obj.sess.query(obj.Deliverables).filter(obj.Deliverables.class_d == 'SEGY').filter(or_(obj.Deliverables.media == '3592 JA', obj.Deliverables.media == '3592 JC',
                                                                                                    obj.Deliverables.media == '3592 JA multiple', obj.Deliverables.media =='3592 JC multiple')).all()
    return result

def return_SEGD_QC_log_path(obj, tape_no, set_no):
    result = obj.sess.query(obj.SEGD_qc).filter(and_(obj.SEGD_qc.tape_no == tape_no, obj.SEGD_qc.set_no == set_no)).first()
    return result.log_path

def return_shipment_content_from_number(obj,shipment_no):
    result = obj.sess.query(obj.Media_list).filter(obj.Media_list.shipment_no == shipment_no).order_by(obj.Media_list.box_no).order_by(obj.Media_list.reel_no).all()
    return result


def get_all_SEGD_QC_for_deliverable(obj,deliverable_id):
    obj_list = obj.sess.query(obj.SEGD_qc).filter(obj.SEGD_qc.deliverable_id == deliverable_id).order_by(obj.SEGD_qc.set_no).order_by(obj.SEGD_qc.tape_no).all()
    return obj_list


def get_all_SEGY_write_objects(obj):
    obj_list = obj.sess.query(obj.SEGY_write).all()
    return obj_list

def get_all_SEGY_qc_objects(obj, deliverable_id):
    obj_list = obj.sess.query(obj.SEGY_QC_on_disk).filter(obj.SEGY_QC_on_disk.deliverable_id == deliverable_id).all()
    return obj_list

def get_all_segd_objects_for_set_checked_before(obj,deliverable_id, set_no):
    obj_list = obj.sess.query(obj.SEGD_qc).filter(obj.SEGD_qc.deliverable_id == deliverable_id).filter(obj.SEGD_qc.set_no == set_no).filter(obj.SEGD_qc.qc_status == True).all()
    return obj_list

if __name__=="__main__":
    fetch_deliverables_list()
