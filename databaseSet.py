# -*- coding: utf-8 -*-

"""
Module implementing MainWindow.
"""
from PyQt5.QtCore import pyqtSlot, pyqtSignal
from PyQt5.QtWidgets import QMainWindow, QMessageBox

from Ui_databaseSet import Ui_MainWindow


class MainWindow(QMainWindow, Ui_MainWindow):
    """
    Class documentation goes here.
    """
    settingsChanged = pyqtSignal(list)
    def __init__(self, parent=None):
        """
        Constructor
        
        @param parent reference to the parent widget (defaults to None)
        @type QWidget (optional)
        """
        super(MainWindow, self).__init__(parent)
        self.setupUi(self)

    @pyqtSlot()
    def on_pushButton_clicked(self):
        """
        Slot documentation goes here.
        """
        host = self.lineEdit.text()
        port = self.lineEdit_2.text()
        user = self.lineEdit_3.text()
        passward = self.lineEdit_4.text()
        database = self.lineEdit_5.text()
        multi_size = self.lineEdit_6.text()
        arr = [host, port, user, passward, database, multi_size]
        self.emitSettings(arr)
        QMessageBox.information(self, '提示框', '数据库参数保存完成，即将返回主界面...', QMessageBox.Ok)
        self.close()

    def emitSettings(self, param):
        # 发送参数设置信号
        self.settingsChanged.emit(param)
if __name__ == '__main__':
    import sys
    from PyQt5.QtWidgets import QMainWindow, QApplication

    app = QApplication(sys.argv)
    ui = MainWindow()
    ui.show()
    sys.exit(app.exec_())
