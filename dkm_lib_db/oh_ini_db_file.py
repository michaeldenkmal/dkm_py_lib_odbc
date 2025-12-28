__author__ = 'michael'

from dkm_lib_pure import uiniFile
import os

from dkm_lib_db.db_types import DKM_DB_TYPE

# import win32com.clientuiniFile.py
IS_WIN32_COM_AVAILABLE=True

try:
    # noinspection PyUnresolvedReferences
    import win32com
except ImportError:
    IS_WIN32_COM_AVAILABLE=False


# noinspection PyPep8Naming
def _decryptOhPwd(pwd):
    """
    folgendes muss registriert sein: "D:\projekte\csharp\dkmlib\libCsDkmglobal\pyOhDecrypt\pyOhDecrypt.csproj"
    Zielplattform muss 64 bit sein sonst geht nix
    :param pwd:
    :return:
    """
    # noinspection PyUnresolvedReferences
    if not IS_WIN32_COM_AVAILABLE:
        raise Exception("Verschlüsselte Passwörter können nur mit Windows ausgelesen werden")
    # noinspection PyUnresolvedReferences
    pyOhDecrypt = win32com.client.Dispatch("pyOhDecrypt.PyOhDecrypt")
    return pyOhDecrypt.decodeOhPwd(pwd)

class TOhIniDBFile:

    def __init__(self, offh_ini_file_path):
        self.userName:str=""
        self.password:str=""
        self.database:str=""
        self.password_enc:str=""
        self.host:str=""
        self.bin:int =0
        self.bin_fakt=0
        self.read_ini_file(offh_ini_file_path)
        # TODO: anpassen wegen Mysql - CAO
        self.db_type:DKM_DB_TYPE="mssql"


    def read_ini_file(self, offh_ini_file_path):
        ini = uiniFile.TIniFile(offh_ini_file_path)

        def get_int(section:str, val_name:str)->int:
            szint = ini.readEntry(section, val_name)
            try:
                return int(szint)
            except ValueError as e:
                raise Exception(f"section={section}, val_name={val_name}, val={szint}: {e}")

        sect="DATABASEPARAMS"
        #write_batch_line("DRIVERNAME",ini.readEntry(sect,"DRIVERNAME"))
        self.userName = ini.readEntry(sect,"USER NAME")
        self.password = ini.readEntry(sect,"PASSWORD")
        self.database = ini.readEntry(sect,"DATABASE NAME")
        self.host = ini.readEntry(sect,"SERVER NAME")
        self.password_enc = ini.readEntry(sect,"PASSWORD_ENC")
        if (self.password == "") or (self.password is None):
            self.password = _decryptOhPwd(self.password_enc)
        #[ID]
        sect_id="ID"
        #MTGL_BIN=3101
        self.bin = get_int(sect_id, "MTGL_BIN")
        #BINFAKT=10000
        self.bin_fakt = get_int(sect_id, "BINFAKT")


def get_test_offh_ini_file_path():
    return os.path.join(os.environ["OFFICEH_ROOT"],"delphi","exe","offh.ini")


#def createTestDbConf(fnCreateConnInfoFromIniFile):
#    return fnCreateConnInfoFromIniFile(TOhIniDBFile(getTestOffhIniFilePath()))

#def createDbConf(fnCreateConnInfoFromIniFile, iniFilePath):
#    return fnCreateConnInfoFromIniFile(TOhIniDBFile(iniFilePath))
