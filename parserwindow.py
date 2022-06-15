import winsound

from PyQt5 import QtCore, QtGui, QtWidgets

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt, QThread, pyqtSignal
import pymysql.cursors
from PyQt5.QtWinExtras import QtWin
from sshtunnel import SSHTunnelForwarder


class DataParser(QThread):
    data_signal = pyqtSignal(list)

    def __init__(self, mainwindow):
        super(DataParser, self).__init__()
        self._date = ''
        self._nameProg = ''
        self._start = ''
        self._flag = True
        self._list = []
        self.mainwindow = mainwindow
        self._countSqlEx = 0  # количество записей из базы

    def run(self):

        frequency = 4000  # Set Frequency To 2500 Hertz
        duration = 1000  # Set Duration To 1000 ms == 1 second

        a = 0
        b = 0

        while (self._flag):
            try:
                with SSHTunnelForwarder(
                        ('141.8.192.82', 22),
                        ssh_username='a0662123',
                        ssh_password='acwihaekef',
                        remote_bind_address=('127.0.0.1', 3306)) as tunnel:

                    conn = pymysql.connect(
                        host='localhost',
                        port=tunnel.local_bind_port,
                        user='a0662123_parserdb',
                        password='Accountforparse2000',
                        db='a0662123_parserdb',
                        charset='utf8mb4',
                        cursorclass=pymysql.cursors.DictCursor)
                    cursor = conn.cursor()
                    # res = cursor.execute("SELECT * FROM dateparse ORDER BY id DESC").fetchall()
                    cursor.execute("SELECT * FROM dateparse ORDER BY id DESC")
                    countSqlEx = cursor.rowcount  # len(res)

                    if (self._countSqlEx < countSqlEx) or (a == b):
                        b += 1
                        self.mainwindow.tableWidget.setRowCount(0)
                        self._countSqlEx = countSqlEx
                        res = cursor.fetchall()


                        for row_number, row in enumerate(res):
                            self.mainwindow.tableWidget.insertRow(row_number)
                            cell1 = QtWidgets.QTableWidgetItem(str(row['id']))
                            cell2 = QtWidgets.QTableWidgetItem(str(row['col_a']))
                            cell3 = QtWidgets.QTableWidgetItem(str(row['col_b']))
                            cell4 = QtWidgets.QTableWidgetItem(str(row['fiz_long']))
                            cell5 = QtWidgets.QTableWidgetItem(str(row['fiz_short']))
                            cell6 = QtWidgets.QTableWidgetItem(str(row['yur_long']))
                            cell7 = QtWidgets.QTableWidgetItem(str(row['yur_short']))
                            cell8 = QtWidgets.QTableWidgetItem(str(row['systime']))

                            cell1.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            cell2.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            cell3.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            cell4.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            cell5.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            cell6.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            cell7.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            cell8.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)


                            self.mainwindow.tableWidget.setItem(row_number, 0, cell1)
                            self.mainwindow.tableWidget.setItem(row_number, 1, cell2)
                            self.mainwindow.tableWidget.setItem(row_number, 2, cell3)
                            self.mainwindow.tableWidget.setItem(row_number, 3, cell4)
                            self.mainwindow.tableWidget.setItem(row_number, 4, cell5)
                            self.mainwindow.tableWidget.setItem(row_number, 5, cell6)
                            self.mainwindow.tableWidget.setItem(row_number, 6, cell7)
                            self.mainwindow.tableWidget.setItem(row_number, 7, cell8)
                            if row_number == 0:
                                self.mainwindow.label_1.setText(str(row['col_a']))
                                self.mainwindow.label_2.setText(str(row['col_b']))
                        self.mainwindow.tableWidget.resizeColumnsToContents()
                        if (float(res[0]['col_a']) != float(0)) \
                                or (float(res[0]['col_b']) != float(0)):
                            QApplication.alert(MainWindow, 3000)
                            try:
                                winsound.PlaySound('yvedomlenie.wav', winsound.SND_FILENAME)
                            except:
                                QApplication.instance().beep()
                    conn.close()

                # self.data_signal.emit(self._list)  # отдаем список в основной поток
            # except Exception as error:
            except:
                # print(traceback.format_exc())
                pass

            # finally:
            #     conn.close()

            self.msleep(2000)

        # Наследуемся от QMainWindow


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(853, 483)

        try:
            appid = '11.22.3343'
            QtWin.setCurrentProcessExplicitAppUserModelID(appid)
            MainWindow.setWindowIcon(QtGui.QIcon('moex_ico.ico'))
        except ImportError:
            pass

        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setMinimumSize(QtCore.QSize(835, 0))
        self.centralwidget.setObjectName("centralwidget")
        self.verticalLayout = QtWidgets.QVBoxLayout(self.centralwidget)
        self.verticalLayout.setObjectName("verticalLayout")
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label_1 = QtWidgets.QLabel(self.centralwidget)
        font = QtGui.QFont()
        font.setPointSize(20)
        font.setBold(True)
        font.setWeight(75)
        self.label_1.setFont(font)
        self.label_1.setStyleSheet("background-color: rgb(76, 76, 76);color: rgb(0, 255, 0)")
        self.label_1.setAlignment(QtCore.Qt.AlignCenter)
        self.label_1.setObjectName("label_1")
        self.label_1.setMargin(30)
        self.horizontalLayout.addWidget(self.label_1)
        self.label_2 = QtWidgets.QLabel(self.centralwidget)
        palette = QtGui.QPalette()
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active, QtGui.QPalette.WindowText, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active, QtGui.QPalette.Button, brush)
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active, QtGui.QPalette.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active, QtGui.QPalette.ButtonText, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active, QtGui.QPalette.Base, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active, QtGui.QPalette.Window, brush)
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive, QtGui.QPalette.WindowText, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive, QtGui.QPalette.Button, brush)
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive, QtGui.QPalette.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive, QtGui.QPalette.ButtonText, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive, QtGui.QPalette.Base, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive, QtGui.QPalette.Window, brush)
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled, QtGui.QPalette.WindowText, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled, QtGui.QPalette.Button, brush)
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled, QtGui.QPalette.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled, QtGui.QPalette.ButtonText, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled, QtGui.QPalette.Base, brush)
        brush = QtGui.QBrush(QtGui.QColor(76, 76, 76))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled, QtGui.QPalette.Window, brush)
        self.label_2.setPalette(palette)
        font = QtGui.QFont()
        font.setPointSize(20)
        font.setBold(True)

        font.setWeight(75)
        self.label_2.setFont(font)
        self.label_2.setStyleSheet("background-color: rgb(76, 76, 76);color: rgb(255, 0, 0)")
        self.label_2.setAlignment(QtCore.Qt.AlignCenter)
        self.label_2.setObjectName("label_2")
        self.horizontalLayout.addWidget(self.label_2)
        self.verticalLayout.addLayout(self.horizontalLayout)
        self.tableWidget = QtWidgets.QTableWidget(self.centralwidget)
        self.tableWidget.setEnabled(True)

        # self.tableWidget.setStyleSheet("gridline-color: rgb(212, 212, 212);\n"
        #                                "background-color: rgb(141, 141, 141);\n"
        #                                "_label {color: #fff;  font-size: 18px;};")


        self.tableWidget.setStyleSheet("color: rgb(227, 227, 227);\n"
                                       "background-color: rgb(150, 150, 150);"
                                       "font: 10pt \"Times New Roman\";\n")

        # self.tableWidget.horizontalHeader().setStyleSheet("background-color: rgb(155, 155, 155);")



        # self.tableWidget.setStyleSheet("""
        #             QTableWidget{
        #                 background-color: rgb(141, 141, 141);
        #                 color: #fff;
        #             }
        #
        #         """)

        self.tableWidget.setObjectName("tableWidget")

        # item1 = QtWidgets.QTableWidgetItem()
        # item1.setBackground(QtGui.QColor(255, 0, 0))
        # self.tableWidget.setHorizontalHeaderItem(0, item1)


        self.tableWidget.setColumnCount(8)
        self.tableWidget.setRowCount(0)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(3, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(4, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(5, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(6, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(7, item)
        self.verticalLayout.addWidget(self.tableWidget)
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        self.thread = DataParser(mainwindow=self)
        self.thread.start()

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MOEX. Просмотр результатов"))
        self.label_1.setText(_translate("MainWindow", "0.0"))
        self.label_2.setText(_translate("MainWindow", "0.0"))
        item = self.tableWidget.horizontalHeaderItem(0)
        item.setText(_translate("MainWindow", "№"))

        item = self.tableWidget.horizontalHeaderItem(1)
        item.setText(_translate("MainWindow", "Значение\n"
                                              " А"))
        item = self.tableWidget.horizontalHeaderItem(2)
        item.setText(_translate("MainWindow", "Значение\n"
                                              " B"))
        item = self.tableWidget.horizontalHeaderItem(3)
        item.setText(_translate("MainWindow", "Длинные\n"
                                              "позиции\n"
                                              "физ лиц"))
        item = self.tableWidget.horizontalHeaderItem(4)
        item.setText(_translate("MainWindow", "Короткие\n"
                                              "позиции\n"
                                              "физ лиц"))
        item = self.tableWidget.horizontalHeaderItem(5)
        item.setText(_translate("MainWindow", "Длинные\n"
                                              "позиции\n"
                                              "юр лиц"))
        item = self.tableWidget.horizontalHeaderItem(6)
        item.setText(_translate("MainWindow", "Короткие\n"
                                              "позиции\n"
                                              "юр лиц"))
        item = self.tableWidget.horizontalHeaderItem(7)
        item.setText(_translate("MainWindow", "Время\n"
                                              "подсчета"))


if __name__ == "__main__":
    import sys

    app = QtWidgets.QApplication(sys.argv)

    app.setStyle(QtWidgets.QStyleFactory.create('Fusion'))  # won't work on windows style.

    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())
