from PyQt4 import QtGui,QtCore
from general_functions.general_functions import create_center_data,create_central_labels
from GUI_classes.class_pop_up_combo_box import pop_up_combo_box
from configuration.Tool_tips import tool_tips_mapper_dict
from class_SEGD_QC_form import SEGD_QC_Form
from dug_ops.DUG_ops import run_command_on_tape_server
from class_SEGY_write_form_multiple import segy_write_multiple

class Tape_drive_dashboard(QtGui.QWidget):

    closed = QtCore.pyqtSignal()


    def __init__(self, parent):
        # define the top window

        super(Tape_drive_dashboard, self).__init__()

        self.parent = parent
        self.tool_tip_dict = tool_tips_mapper_dict['tape_dashboard']
        self.grid = QtGui.QGridLayout()
        self.tape_operation_manager = self.parent.tape_operation_manager
        self.tape_service = self.tape_operation_manager.tape_service

        self.home_screen()
        self.create_widgets_dict()
        self.set_dst_labels()
        self.set_dst_widgets()

        self.setLayout(self.grid)
        self.show()

    def home_screen(self):
        self.grid.addWidget(create_central_labels(str(self.tape_service.use_location)), 0, 1, 1,
                            len(self.tape_service.available_dst))

        self.pb_refresh = QtGui.QPushButton("Refresh all")
        self.pb_refresh.setToolTip(self.tool_tip_dict['refresh'])
        self.pb_refresh.clicked.connect(self.refresh_all)

        self.pb_rewind = QtGui.QPushButton("Rewind")
        self.pb_rewind.setToolTip(self.tool_tip_dict['rewind'])
        self.pb_rewind.clicked.connect(self.rewind_dst)

        self.pb_eject = QtGui.QPushButton("Eject")
        self.pb_eject.setToolTip(self.tool_tip_dict['eject'])
        self.pb_eject.clicked.connect(self.eject_dst)

        self.pb_segd_qc = QtGui.QPushButton("SEGD QC")
        self.pb_segd_qc.setToolTip(self.tool_tip_dict['segd_qc'])
        self.pb_segd_qc.clicked.connect(self.segd_qc)

        self.pb_segy = QtGui.QPushButton("SEGY write")
        self.pb_segy.setToolTip(self.tool_tip_dict['segy_write'])
        self.pb_segy.clicked.connect(self.segy_write)

        self.pb_quit = QtGui.QPushButton("Exit")
        self.pb_quit.setToolTip(self.tool_tip_dict['exit'])
        self.pb_quit.clicked.connect(self.closeEvent)

        self.grid.addWidget(self.pb_refresh, 2, 0)
        self.grid.addWidget(self.pb_rewind, 3, 0)
        self.grid.addWidget(self.pb_eject, 4, 0)
        self.grid.addWidget(self.pb_segd_qc, 5, 0)
        self.grid.addWidget(self.pb_segy, 6, 0)
        self.grid.addWidget(self.pb_quit, 7, 0)

    def create_widgets_dict(self):
        self.dst_widget_dict = {}
        self.dst_label_dict = {}
        self.dst_position_dict = {}
        for dst in self.tape_service.available_dst:
            text_edit_obj = QtGui.QTextEdit()
            label = create_center_data(dst)
            position = dst[len(dst)-1]
            widject_dict_obj = {dst:text_edit_obj}
            label_dict_obj = {dst:label}
            position_dict_obj = {dst:position}
            self.dst_widget_dict.update(widject_dict_obj)
            self.dst_label_dict.update(label_dict_obj)
            self.dst_position_dict.update(position_dict_obj)

    def set_dst_widgets(self):
        for dst in self.dst_label_dict.keys():
            self.grid.addWidget(self.dst_widget_dict[dst], 2, int(self.dst_position_dict[dst]) + 1,6,1)

    def set_dst_labels(self):
        for dst in self.dst_label_dict.keys():
            self.grid.addWidget(self.dst_label_dict[dst],1,int(self.dst_position_dict[dst])+1)

    def refresh_all(self):
        for dst in self.tape_service.available_dst:
            self.dst_widget_dict[dst].setText("")
            cmd = self.tape_service.dst_status_command_dict[dst]
            dev_to_use = str(dst)
            return_data = run_command_on_tape_server(self.tape_operation_manager.DUG_connection_obj, cmd, dev_to_use)
            if return_data == None:
                print "Noting to update"
                pass
            else:
                for line in return_data.readlines():
                    self.dst_widget_dict[dst].append(line.rstrip("\n"))
                    print line
        print "Refresh complete.. "


    def rewind_dst(self):
        status = self.tape_operation_manager.get_verified_tape_drive(self)
        if status is True:
            dev_to_use = str(self.tape_operation_manager.tape_drive)
            self.tape_operation_manager.tape_service.run_cmd(
                self.tape_operation_manager.tape_service.dst_rewind_command_dict[
                    self.tape_operation_manager.tape_drive], dev_to_use)


    def eject_dst(self):
        status = self.tape_operation_manager.get_verified_tape_drive(self)
        if status is True:
            dev_to_use = str(self.tape_operation_manager.tape_drive)
            self.tape_operation_manager.tape_service.run_cmd(
                self.tape_operation_manager.tape_service.dst_eject_command_dict[
                    self.tape_operation_manager.tape_drive],dev_to_use)

    def choose_tape_drive(self, ops):
        combo_list = self.tape_operation_manager.tape_service.available_dst
        self.pop_up_combo_box = pop_up_combo_box(self, "Select Tape drive", combo_list, 'drive', ops)
        self.pop_up_combo_box.closed.connect(self.show)
        self.pop_up_combo_box.show()


    def segd_qc(self):
        self.SEGD_QC_form = SEGD_QC_Form(self)
        self.SEGD_QC_form.setMinimumHeight(200)
        self.SEGD_QC_form.setMinimumWidth(100)
        self.SEGD_QC_form.show()



    def segy_write(self):
        self.SEGY_write_form = segy_write_multiple(self)
        self.SEGY_write_form.setMinimumWidth(400)
        self.SEGY_write_form.setMinimumHeight(700)
        self.SEGY_write_form.show()


    def SEGY_write_execute(self,reel_no,file_list):
        self.tape_operation_manager.service_class.set_SEGY_path_multiple(file_list)
        self.tape_operation_manager.set_tape_name(reel_no)
        self.tape_operation_manager.service_class.set_logpath()
        self.tape_operation_manager.service_class.chk_and_run()


    def no_use(self):
        pass

    def closeEvent(self, event):
        self.parent.tape_dashboard_visible = False
        self.closed.emit()
        self.hide()


            #
    # def choose_set_no(self,ops):
    #     combo_list = self.tape_operation_manager.get_deliverable_set_list()
    #     self.pop_up_combo_box = pop_up_combo_box(self, "Select set number", combo_list, 'set',ops)
    #     self.pop_up_combo_box.closed.connect(self.show)
    #     self.pop_up_combo_box.show()
    #
    #
    # def choose_sequence_name(self,ops):
    #     if self.tape_operation_manager.service_class_name == 'SEGD':
    #         combo_list = self.tape_operation_manager.service_class.get_list_of_available_segd_seq()
    #         self.pop_up_combo_box = pop_up_combo_box(self, "Select SEGD Dir", combo_list, 'seq',ops)
    #     elif self.tape_operation_manager.service_class_name == 'SEGY':
    #         combo_list = self.tape_operation_manager.service_class.get_list_of_files_where_ondisk_qc_is_approved()
    #         self.pop_up_combo_box = pop_up_combo_box(self, "Select SEGY file", combo_list, 'file', ops)
    #     self.pop_up_combo_box.closed.connect(self.show)
    #     self.pop_up_combo_box.show()
    #
    #
    # def choose_tape_name(self,ops):
    #     combo_list = self.tape_operation_manager.service_class.get_list_of_applicable_segd_tapes()
    #     self.pop_up_combo_box = pop_up_combo_box(self, "Select Tape", combo_list, 'tape',ops)
    #     self.pop_up_combo_box.closed.connect(self.show)
    #     self.pop_up_combo_box.show()

    # def set_attribute(self,attribute,caller,ops):
    #     if ops =='SEGD_QC':
    #         if caller == 'seq':
    #            if self.tape_operation_manager.service_class_name == 'SEGD':
    #                self.tape_operation_manager.set_seq_name(attribute)
    #                self.tape_operation_manager.service_class.set_SEGD_path(attribute)
    #                self.choose_tape_name(ops)
    #            else:
    #                pass
    #         elif caller == 'tape':
    #             self.tape_operation_manager.set_tape_name(attribute)
    #             self.tape_operation_manager.service_class.set_logfile()
    #             typed_label = self.tape_operation_manager.get_confirmation_for_tape_label(self)
    #             if typed_label == self.tape_operation_manager.tape_name:
    #                 self.tape_operation_manager.service_class.run()
    #             else:
    #                 print "The typed tape label does not match the selected label.."
    #         elif caller == 'drive':
    #             self.tape_operation_manager.set_tape_drive(attribute)
    #             print "Drive selected"
    #             self.choose_deliverable(ops)
    #         elif caller == 'deliverable':
    #             self.tape_operation_manager.set_deliverable(attribute)
    #             self.choose_set_no(ops)
    #         elif caller == 'set':
    #             self.tape_operation_manager.set_working_set(attribute)
    #             self.choose_sequence_name(ops)
        # elif ops == 'SEGY_W':
        #     if caller == 'drive':
        #         self.tape_operation_manager.set_tape_drive(attribute)
        #         self.choose_deliverable(ops)
        #     elif caller == 'deliverable':
        #         self.tape_operation_manager.set_deliverable(attribute)
        #         self.choose_set_no(ops)
        #     elif caller == 'set':
        #         self.tape_operation_manager.set_working_set(attribute)
        #         self.choose_sequence_name(ops)
        #     elif caller == 'file':
        #         # Now we need to verify a few things before we go and write the tape
        #         # 1. Has this tape been written already
        #         # 2. Is the tape name the same as the one in EBCDIC
        #         self.segy_name = attribute
        #         self.tape_operation_manager.service_class.set_SEGY_path(attribute)
        #         self.get_reel_number(ops)
        #     elif caller == 'reel_no':
        #         reel_no = attribute
        #         #check reel number
        #         reel_verification = self.tape_operation_manager.service_class.verify_reel_number_before_segy_write(self.segy_name
        #                                                                                                            ,reel_no)
        #         print reel_verification
        #         if reel_verification:
        #             self.parent.print_to_run_log(str("Now writing SEGY : " + attribute + " to tape: " + reel_no))
        #             print str("Now writing SEGY : " + attribute + " to tape: " + reel_no)
        #             self.tape_operation_manager.set_tape_name(reel_no)
        #             self.tape_operation_manager.service_class.set_logpath()
        #             self.tape_operation_manager.service_class.chk_and_run()
        #         else:
        #             print "test"
        #             message = str("Reel no mismatch , aborting ....please retry")
        #             self.message_box = pop_up_message_box(message=message,type="Critical")
        #             self.message_box.closed.connect(lambda :self.get_reel_number(ops))
        #             self.message_box.show()

    # def get_reel_number(self,ops):
    #     reel_no = get_item_through_dialogue(self, str("Please enter Reel no"))
    #     self.set_attribute(reel_no,'reel_no',ops)



#


