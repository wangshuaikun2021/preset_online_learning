# -*- coding: utf-8 -*-

"""
Module implementing MainWindow.
"""
import os

from PyQt5.QtCore import pyqtSlot, QUrl, QThread, pyqtSignal
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtWidgets import QMainWindow, QAction, QFileDialog

from Mymain import mainFunc
from Ui_online_updates import Ui_MainWindow



class WorkerThread(QThread):
    progressChanged = pyqtSignal(str)

    def __init__(self, last_id, save_path_start, save_path_preset, multi_size, update, localhost, port, user, passwd,
                 db, num_limit, parent=None):
        super().__init__(parent)
        self.last_id = last_id
        self.save_path_start = save_path_start
        self.save_path_preset = save_path_preset
        self.multi_size = multi_size
        self.update = update
        self.localhost = localhost
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.num_limit = num_limit

    def run(self):
        mainFunc(self.last_id, self.save_path_start, self.save_path_preset, self.multi_size, self.update,
                 self.localhost, self.port, self.user, self.passwd, self.db, self.num_limit)
        # mainFunc(last_id, save_path_start, save_path_preset, size=8, update_t='15:15', host='localhost', port=3306,
        #          user='root', passwd='root', db='shougang_cold_rolling', num_limit=2000)
        self.progressChanged.emit('Done')


class MainWindow(QMainWindow, Ui_MainWindow):
    """
    Class documentation goes here.
    """

    def __init__(self, parent=None):
        """
        Constructor
        
        @param parent reference to the parent widget (defaults to None)
        @type QWidget (optional)
        """
        super(MainWindow, self).__init__(parent)
        self.worker_thread = None

        self.last_id = 10000
        self.update = '05:00:00'  # 程序运行
        self.isr_limit = '0.51'  # ISR限值
        self.forget = '1000'  # 遗忘卷数

        self.localhost = 'localhost'
        self.port = '3306'
        self.user = 'root'
        self.passwd = '123456'
        self.db = 'shougang'
        self.multi_size = 8
        self.setupUi(self)

    @pyqtSlot()
    def on_actionOpen_Help_Doc_triggered(self):
        """
        Slot documentation goes here.
        """
        file_path = r"在线更新.md"  # 替换为你的Markdown文件路径
        url = QUrl.fromLocalFile(file_path)
        QDesktopServices.openUrl(url)

    @pyqtSlot()
    def on_actiondatabase_triggered(self):
        """
        Slot documentation goes here.
        """
        print('database')
        from databaseSet import MainWindow
        self.database_set = MainWindow()
        self.database_set.settingsChanged.connect(self.process_db)
        self.database_set.show()

    def process_db(self, param):
        print(param)
        self.localhost, self.port, self.user, self.passwd, self.db, self.multi_size = param
        self.port = int(self.port)

    @pyqtSlot()
    def on_actionpreset_triggered(self):
        """
        Slot documentation goes here.
        """
        print('preset')
        from presetSet import MainWindow
        self.preset_set = MainWindow()
        self.preset_set.settingsChanged.connect(self.process_preset)
        self.preset_set.show()

    def process_preset(self, param):
        print(param)
        self.forget, self.isr_limit, self.update = param

    @pyqtSlot()
    def on_actionExport_triggered(self):
        """
        Slot documentation goes here.
        """
        print('export')
        from files_process import MainWindow
        self.export_set = MainWindow()
        self.export_set.show()


    @pyqtSlot()
    def on_pushButton_clicked(self):
        """
        run
        """
        self.last_id = int(self.lineEdit.text())
        self.port = int(self.port)
        self.multi_size = int(self.multi_size)
        self.lineEdit_4.setText("已运行。请勿执行任何操作！谢谢配合！")
        # mainFunc(self.last_id, self.save_path_start, self.save_path_preset, self.multi_size, self.update,
        #          self.localhost, self.port, self.user, self.passwd, self.db)
        if self.worker_thread is None or not self.worker_thread.isRunning():
            self.worker_thread = WorkerThread(self.last_id, self.save_path_start, self.save_path_preset,
                                              self.multi_size, self.update, self.localhost, self.port, self.user,
                                              self.passwd, self.db, self.forget)
            self.worker_thread.start()

    @pyqtSlot()
    def on_pushButton_2_clicked(self):
        """
        初值表保存路径

        不出意外的话，变量名长这样
        ['steel','bpph','h_in','h_out','width','policyNo',
               'SY_PTM_STD1_WR_BendRoll_SV',
               'SY_PTM_STD2_WR_BendRoll_SV',
               'SY_PTM_STD3_WR_BendRoll_SV',
               'SY_PTM_STD4_WR_BendRoll_SV',
               'SY_PTM_STD5_WR_BendRoll_SV',
               'SY_PTM_STD1_IMR_BendRoll_SV',
               'SY_PTM_STD2_IMR_BendRoll_SV',
               'SY_PTM_STD3_IMR_BendRoll_SV',
               'SY_PTM_STD4_IMR_BendRoll_SV',
               'SY_PTM_STD5_IMR_BendRoll_SV',
               'SY_PTM_STD1_IMR_TopRollShift_SV',
               'SY_PTM_STD2_IMR_TopRollShift_SV',
               'SY_PTM_STD3_IMR_TopRollShift_SV',
               'SY_PTM_STD4_IMR_TopRollShift_SV',
               'SY_PTM_STD5_IMR_TopRollShift_SV',
               'iu_mean',
               'iu50_mean',
               'iu100_mean',
               'rf_mean_1',
               'rf50_mean_1',
               'rfMid_mean_1',
               'rf_mean_2',
               'rf50_mean_2',
               'rfMid_mean_2',
               'rf_mean_3',
               'rf50_mean_3',
               'rfMid_mean_3',
               'rf_mean_4',
               'rf50_mean_4',
               'rfMid_mean_4',
               'rf_mean_5',
               'rf50_mean_5',
               'rfMid_mean_5',
               'C40_average']
        """
        print("初值表保存路径")
        self.save_path_start = QFileDialog.getExistingDirectory(self, "初值表保存路径", os.getcwd())
        self.lineEdit_2.setText(self.save_path_start)

        files = os.listdir(self.save_path_start)

        # 构建文件路径列表，并获取每个文件的修改时间
        file_paths = [os.path.join(self.save_path_start, file) for file in files]
        file_times = [os.path.getmtime(file) for file in file_paths]

        # 将文件列表按照修改时间排序
        files = [file for _, file in sorted(zip(file_times, files))]
        name = files[-1].split('.')[0]
        self.lineEdit.setText(name)
        # self.last_id = int(name)

    @pyqtSlot()
    def on_pushButton_3_clicked(self):
        """
        设定表保存路径
        """
        print("设定表保存路径")
        self.save_path_preset = QFileDialog.getExistingDirectory(self, "设定表保存路径", os.getcwd())
        self.lineEdit_3.setText(self.save_path_preset)


if __name__ == '__main__':
    import sys
    from PyQt5.QtWidgets import QMainWindow, QApplication

    app = QApplication(sys.argv)
    ui = MainWindow()
    ui.show()
    sys.exit(app.exec_())
