from sqlalchemy import create_engine,MetaData, Table
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker,scoped_session
import pickle
from sqlalchemy.ext.automap import automap_base
from configuration import conn_config_file, orca_schema_name, deliverables_qc_schema_name,echo_mode
import os

from sqlalchemy import event
from sqlalchemy.engine import Engine

import time
import logging
from app_log import  stream_formatter
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter(stream_formatter)
console.setFormatter(formatter)
logger.addHandler(console)

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())
    logger.debug("Start Query: %s" % statement)

@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    logger.debug("Query Complete!")
    logger.debug("Total Time: %f" % total)


class db_connection_obj(object):

    def __init__(self):
        self.initialize_db_engine()
        if self.db_engine_status ==True:
            self.initialize_db_Session()
            self.initialize_db_working_session()
            self.initialize_scoped_session()
            self.initialize_metadata()
            self.initialize_base()
            self.initialize_all_dao()
        else:
            logger.error("Cannot proceed as the the database engine is not setup")


    def initialize_all_dao(self):
        self.initialize_dao_project_info()
        self.initialize_dao_deliverables_definition()
        self.initialize_dao_deliverables()
        self.initialize_dao_deliverables_data_dir()
        self.initialize_dao_deliverables_qc_dir()
        self.initialize_dao_raw_seq_info()
        self.initialize_dao_shipments()
        self.initialize_dao_tape()
        self.initialize_dao_segd_qc()
        self.initialize_dao_media_list()
        self.initialize_dao_usb_list()
        self.initialize_dao_usb_files()
        self.initialize_dao_change_log()
        self.initialize_dao_line()
        self.initialize_dao_seq_segy_qc_on_disk()
        self.initialize_dao_segy_write()

    def initialize_db_engine(self):
        #file_path = os.path.join(Path(os.getcwd()).parent, conn_config_file) # use this string to test the stand alone connection module
        file_path = os.path.join(os.getcwd(), conn_config_file) # use this string for production mode in the application
        logger.info("config_file: " + file_path)
        file_handler = open(file_path, "rb")
        obj= pickle.load(file_handler)
        file_handler.close()
        db_name = obj.db_name
        db_user = obj.db_user
        db_pword = obj.db_pword
        db_port = obj.db_port

        host_IP = obj.host_IP
        host_user = obj.host_user
        host_pword = obj.host_pword
        # with SSHTunnelForwarder((host_IP, 22), ssh_password=host_pword, ssh_username=host_user,
        #                         remote_bind_address=('127.0.0.1', 5432),local_bind_address=('0.0.0.0', 1111))as server:
        engine_definition = str('postgresql://'+ db_user + ":"+db_pword + "@127.0.0.1:1111/"+db_name)
        logger.info("Now setting up DB engine ..")
        #return create_engine(engine_definition)
        try:
            self.db_engine =  create_engine(engine_definition, poolclass = QueuePool,echo = echo_mode)
            logger.info("Done.....")
            self.db_engine_status = True
        except Exception as error:
            logger.critical(error)
            self.db_engine_status = False

    def initialize_db_Session(self):
        logger.info("Now setting up the Session ....")
        try:
            self.Session =  sessionmaker(self.db_engine)
            logger.info("Done.....")
        except Exception as e:
            logger.critical("Unable to setup db_session")


    def initialize_db_working_session(self):
        logger.info("Now setting up working database session")
        try:
            self.sess = self.Session()
        except Exception as error:
            logger.critical(error)

    def initialize_scoped_session(self):
        logger.info("Now setting up scoped Session...")
        try:
            self.scoped_session = scoped_session(self.Session)
            logger.info("Done.....")
        except Exception as error:
            logger.critical(error)

    def initialize_metadata(self):
        logger.info("Now initializing schema Metadata")
        try:
            self.metadata_orca = MetaData(schema=orca_schema_name)
            self.metadata_deliverables_qc = MetaData(schema = deliverables_qc_schema_name)
            logger.info("Done")
        except Exception as error:
            logger.critical(error)

    def initialize_base(self):
        logger.info("Initializing bases to create DAO from schema metadata")
        try:
            self.Base_orca = automap_base(metadata = self.metadata_orca)
            self.Base_orca.prepare(self.db_engine, reflect=True)
            self.Base_deliverables_qc = automap_base(metadata = self.metadata_deliverables_qc)
            self.Base_deliverables_qc.prepare(self.db_engine, reflect=True)
            logger.info("Done")
        except Exception as error:
            logger.critical(error)

    def initialize_dao_project_info(self):
        logger.info("Now setting up DAO for Project info....")
        try:
            self.Project_info = self.Base_orca.classes.project_info
            logger.info("Done.......")
        except Exception as error:
            logger.error(error)

    def initialize_dao_deliverables_definition(self):
        self.Deliverables_def = Table('deliverables',self.metadata_deliverables_qc,autoload = True,autoload_with =self.db_engine)

    def initialize_dao_deliverables(self):
        logger.info("Now setting up DAO for deliverables table....")
        try:
            self.Deliverables =  self.Base_deliverables_qc.classes.deliverables
            logger.info("Done.......")
        except Exception as error:
            logger.error(error)

    def initialize_dao_deliverables_data_dir(self):
        logger.info("Now setting up DAO for deliverables data dir table....")
        try:
            self.Deliverables_data_dir = self.Base_deliverables_qc.classes.deliverables_data_dir
            logger.info("Done.......")
        except Exception as error:
            logging.error(error)

    def initialize_dao_deliverables_qc_dir(self):
        logger.info("Now setting up DAO for deliverables qc dir table....")
        try:
            self.Deliverables_qc_dir = self.Base_deliverables_qc.classes.deliverables_qc_dir
            logger.info("Done.......")
        except Exception as error:
            logging.error(error)

    def initialize_dao_raw_seq_info(self):
        logger.info("Now setting up DAO for Raw seq info table....")
        try:
            self.Raw_seq_info = self.Base_deliverables_qc.classes.raw_seq_info
            logger.info("Done.......")
        except Exception as error:
            logging.error(error)

    def initialize_dao_shipments(self):
        logger.info("Now setting up DAO for shipments table....")
        try:
            self.Shipments = self.Base_deliverables_qc.classes.shipment_entries
            logger.info("Done.......")
        except Exception as error:
            logger.error(error)

    def initialize_dao_tape(self):
        logger.info("Now setting up DAO for orca SEGD tape table ......")
        try:
            self.SEGD_tapes = self.Base_orca.classes.tape
            logger.info("Done ......")
        except Exception as error:
            logger.error(error)

    def initialize_dao_segd_qc(self):
        logger.info("Now setting up DAO for orca SEGD tape QC table ......")
        try:
            self.SEGD_qc = self.Base_deliverables_qc.classes.segd_qc
            logger.info("Done ......")
        except Exception as error:
            logger.error(error)

    def initialize_dao_media_list(self):
        logger.info("Now adding the DAO for media list table .....")
        try:
            self.Media_list = self.Base_deliverables_qc.classes.media_list
            logger.info("Done .... ")
        except Exception as error:
            logger.error(error)

    def initialize_dao_usb_list(self):
        logger.info("Now adding the DAO for usb list table .... ")
        try:
            self.USB_list = self.Base_deliverables_qc.classes.usb_list
            logger.info("Done ....")
        except Exception as error:
            logger.error(error)

    def initialize_dao_usb_files(self):
        logger.info("Now adding the DAO for usb files list table ....")
        try:
            self.USB_files = self.Base_deliverables_qc.classes.usb_files
            logger.info("Done ....")
        except Exception as error:
            logger.error(error)

    def initialize_dao_change_log(self):
        logger.info("Now adding DAO for change log table .....")
        try:
            self.change_log = self.Base_deliverables_qc.classes.change_log
            logger.info("Done ...")
        except Exception as error:
            logger.error(error)

    def initialize_dao_line(self):
        logger.info("Now adding DAO for change line table .....")
        try:
            self.Line = self.Base_orca.classes.line
            logger.info("Done ...")
        except Exception as error:
            logger.error(error)

    def initialize_dao_seq_segy_qc_on_disk(self):
        logger.info("Now adding DAO for change SEGY on disk QC  table .....")
        try:
            self.SEGY_QC_on_disk = self.Base_deliverables_qc.classes.seq_segy_qc_on_disk
            logger.info("Done ...")
        except Exception as error:
            logger.error(error)

    def initialize_dao_segy_write(self):
        logger.info("Now adding DAO for SEGY write Table ...")
        try:
            self.SEGY_write = self.Base_deliverables_qc.classes.segy_write
            logger.info("Done ...")
        except Exception as error:
            logger.error(error)


if __name__ == "__main__":
    pass

