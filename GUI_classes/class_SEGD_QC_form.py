from PyQt4 import QtGui,QtCore
from general_functions.general_functions import create_central_labels

class SEGD_QC_Form(QtGui.QWidget):

    def __init__(self,parent):
        super(SEGD_QC_Form, self).__init__()
        #parent
        self.parent = parent


        #grid
        self.grid = QtGui.QGridLayout()
        self.setWindowTitle("SEGD QC Input form")


        # labels

        self.grid.addWidget(create_central_labels('SEGD QC Input'),0,0,1,3)
        self.grid.addWidget(create_central_labels('Select Drive'),1,0)
        self.grid.addWidget(create_central_labels('Select Deliverable'),2,0)
        self.grid.addWidget(create_central_labels('Select line'),3,0)
        self.grid.addWidget(create_central_labels('Select set'),4,0)
        self.grid.addWidget(create_central_labels('Select Tape'),5,0)
        self.grid.addWidget(create_central_labels('Confirm Tape'),6,0)


        # pushbuttons


        self.pb_execute = QtGui.QPushButton('Run')
        self.grid.addWidget(self.pb_execute,6,2)
        self.pb_execute.clicked.connect(self.execute)


        # combo boxes

        self.combo_tape_drive = QtGui.QComboBox()
        self.combo_tape_drive.setObjectName("Tape Drive")
        self.grid.addWidget(self.combo_tape_drive,1,1)
        self.combo_tape_drive.addItems(self.parent.tape_operation_manager.tape_service.available_dst)
        self.combo_tape_drive.setCurrentIndex(-1)
        self.combo_tape_drive.blockSignals(False)
        self.combo_tape_drive.currentIndexChanged.connect(self.tape_drive_selected)



        self.combo_deliverable = QtGui.QComboBox()
        self.combo_deliverable.setObjectName("Deliverable")
        self.grid.addWidget(self.combo_deliverable,2,1)
        self.combo_deliverable.addItems(self.parent.tape_operation_manager.get_available_segd_deliverable_list())
        self.combo_deliverable.setCurrentIndex(-1)
        self.combo_deliverable.blockSignals(False)
        self.combo_deliverable.currentIndexChanged.connect(self.deliverable_selected)



        self.combo_line = QtGui.QComboBox()
        self.combo_line.setObjectName("Line name")
        self.grid.addWidget(self.combo_line,3,1)
        self.combo_line.blockSignals(False)
        self.combo_line.currentIndexChanged.connect(self.line_selected)


        self.combo_set = QtGui.QComboBox()
        self.combo_set.setObjectName("Set no")
        self.grid.addWidget(self.combo_set,4,1)
        self.combo_set.blockSignals(False)
        self.combo_set.currentIndexChanged.connect(self.set_selected)


        self.combo_tape = QtGui.QComboBox()
        self.combo_tape.setObjectName("Tape no")
        self.grid.addWidget(self.combo_tape,5,1)
        self.combo_tape.blockSignals(False)
        self.combo_tape.currentIndexChanged.connect(self.tape_selected)


        self.line_tape = QtGui.QLineEdit()
        self.grid.addWidget(self.line_tape,6,1)


        self.setLayout(self.grid)


    def tape_drive_selected(self):
        dst = str(self.combo_tape_drive.currentText())
        print "Setting tape drive to: " + dst



    def deliverable_selected(self):
        deliverable = str( self.combo_deliverable.currentText())
        print "Deliverable is set to: " + deliverable
        self.parent.tape_operation_manager.set_deliverable(deliverable)
        self.combo_line.clear()
        self.combo_line.addItems(self.parent.tape_operation_manager.service_class.get_list_of_available_segd_seq())
        self.combo_line.setCurrentIndex(-1)


    def line_selected(self):
        if int(self.combo_line.currentIndex()) == -1:
            self.combo_set.clear()
            self.combo_set.setCurrentIndex(-1)
        else:
            line_name = str(self.combo_line.currentText())
            print "Line name is set to: " + line_name
            self.parent.tape_operation_manager.set_seq_name(line_name)
            self.parent.tape_operation_manager.service_class.set_SEGD_path(line_name)
            self.combo_set.clear()
            self.combo_set.addItems(self.parent.tape_operation_manager.get_deliverable_set_list())
            self.combo_set.setCurrentIndex(-1)


    def set_selected(self):
        if int(self.combo_set.currentIndex()) == -1:
            self.combo_tape.clear()
            self.combo_tape.setCurrentIndex(-1)
        else:
            set_no = str(self.combo_set.currentText())
            print "Set no is set to: " + str(set_no)
            self.combo_tape.clear()
            self.combo_tape.addItems(self.parent.tape_operation_manager.service_class.get_list_of_applicable_segd_tapes())
            self.combo_tape.setCurrentIndex(-1)


    def tape_selected(self):
        if int(self.combo_tape.currentIndex()) == -1:
            pass
        else:
            tape = str(self.combo_tape.currentText())
            print "Tape is set to: " + tape



    def execute(self):
        self.combo_list = [
            self.combo_tape_drive,
            self.combo_deliverable,
            self.combo_line,
            self.combo_set,
            self.combo_tape
        ]
        combo_entry_check = True
        for a_combo in self.combo_list:
            if a_combo.currentIndex() == -1:
                combo_entry_check = False
                print str(a_combo.objectName()) + " : Is blank aborting"

        if combo_entry_check == True:
            dst = str(self.combo_tape_drive.currentText())
            self.parent.tape_operation_manager.set_tape_drive(dst)
            deliverable = str(self.combo_deliverable.currentText())
            self.parent.tape_operation_manager.set_deliverable(deliverable)
            line_name = str(self.combo_line.currentText())
            self.parent.tape_operation_manager.set_seq_name(line_name)
            self.parent.tape_operation_manager.service_class.set_SEGD_path(line_name)
            set_no = str(self.combo_set.currentText())
            self.parent.tape_operation_manager.set_working_set(set_no)
            tape = str(self.combo_tape.currentText())
            self.parent.tape_operation_manager.set_tape_name(tape)
            self.parent.tape_operation_manager.service_class.set_logfile()
            # Now perform tape manual vs auto check
            if str(self.combo_tape.currentText()) == str(self.line_tape.text()):
                print " Ok to run"
                self.parent.tape_operation_manager.service_class.run()
                self.close()
            else:
                print "Manual and Db entries for tape do not match"







