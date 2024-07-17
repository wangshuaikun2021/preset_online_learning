# -*- coding: utf-8 -*-

"""
Module implementing MainWindow.
"""
from datetime import date

import numpy as np
import pandas as pd
from PyQt5.QtCore import pyqtSlot, pyqtSignal
from PyQt5.QtWidgets import QMainWindow

from Ui_files_process import Ui_MainWindow


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
        self.setupUi(self)
        self.lineEdit.setReadOnly(True)
        self.lineEdit_3.setReadOnly(True)

    
    @pyqtSlot()
    def on_pushButton_2_clicked(self):
        """
        导出预设定表格.
        """
        try:
            table_path = self.lineEdit_4.text()
            ans_path = self.lineEdit_5.text()
            save_path = self.lineEdit.text()
            old_path = r'E:\myPrograms\preset\预设定表格更新后台运行\预设定表格旧版本'
            current_df = pd.read_excel(fr'{save_path}\STDADIRPASS_AI.xlsx')

            df_table = pd.read_excel(table_path)
            df_ans = pd.read_excel(ans_path)
            new_df = pd.concat([df_table.iloc[:, :17], df_ans.iloc[:, 17:41], df_table.iloc[:, 41:]], axis=1)
            diff_values = (new_df.iloc[:, 2:17] - current_df.iloc[:, 2:17]) / current_df.iloc[:, 2:17]
            diff_values.clip(lower=-0.5, upper=0.5, inplace=True)
            new_df.iloc[:, 2:17] = 0.9 * diff_values * current_df.iloc[:, 2:17] + current_df.iloc[:, 2:17]

            t = str(date.today()).replace('-', '_')
            current_df.to_excel(fr'{old_path}\STDADIRPASS_AI_{t}.xlsx', index=False)
            new_df.to_excel(fr'{save_path}\STDADIRPASS_AI.xlsx', index=False)

        except Exception as e:
            print(e)

    @pyqtSlot()
    def on_pushButton_4_clicked(self):
        """
        编译板坯牌号到刚度等级.
        """
        try:
            save_path = r'E:\myPrograms\preset\预设定表格更新后台运行'
            path_bpph_gd = self.lineEdit_3.text()
            df_bpph_gd = pd.read_excel(path_bpph_gd)
            bpph_gd = dict(zip(df_bpph_gd['ALLOYCODE'],df_bpph_gd['APSKEY']))

            path_cgbj_bpph = self.lineEdit_6.text()
            df_cgbj_bpph = pd.read_excel(path_cgbj_bpph)
            cgbj_bpph = dict(zip(df_cgbj_bpph['出钢标记'],df_cgbj_bpph['板坯牌号']))

            np.save(fr'{save_path}\bpph2gd.npy', bpph_gd)
            np.save(fr'{save_path}\cgbj2bpph.npy', cgbj_bpph)
        except Exception as e:
            print(e)



if __name__ == '__main__':
    import sys
    from PyQt5.QtWidgets import QMainWindow, QApplication

    app = QApplication(sys.argv)
    ui = MainWindow()
    ui.show()
    sys.exit(app.exec_())
