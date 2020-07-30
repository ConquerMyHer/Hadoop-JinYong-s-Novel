#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#define PI 3.1415926

#include <QMainWindow>
#include <QSignalMapper>
#include <QDebug>
#include <QList>
#include <QString>
#include <QStringList>
#include <QPainter>
#include <QPen>
#include <QTextCodec>
#include <QPair>
#include <QFont>
#include <math.h>
#include <QWidget>
#include <QMouseEvent>

#include <QtWidgets/QFileDialog>
#include <QtWidgets/QMessageBox>

#include <relation.h>

#include "sigfiglist.h"
QT_BEGIN_NAMESPACE
namespace Ui { class MainWindow; }
QT_END_NAMESPACE

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    MainWindow(QWidget *parent = nullptr);
    ~MainWindow();

    void loadFile(); // 装入文件
//    void paintRe();

protected:

void paintEvent(QPaintEvent *); // 绘制图像

void mouseMoveEvent(QMouseEvent *event); // 捕捉鼠标事件

private slots:

    void on_FileBrowse_clicked();// 对装载文件的按钮的处理

    void on_vitalNumSlider_valueChanged(int value);// 对显示数量滚动条变化的处理

    void on_sigSizeSlider_valueChanged(int value);// 对疏密程度滚动条变化的处理

    void myMouseMoveHandler(QMouseEvent * e);// 对鼠标点击事件的处理

    void on_transpSlider_valueChanged(int value);

signals:
    void mouseMove(QMouseEvent *event);// 发射的鼠标信号

private:
    Ui::MainWindow *ui;
//    QList<Relation*> reLs;
    sigFigList fgLs;// 包含一个sigFigList类的成员，以对关系和显示进行处理
    bool loaded;// 已经装载文件的信号，应当放入sigFigList更好

    const int hlDistance = 15;//控制高亮显示的检测半径，也即灵敏度
    int curHlInd;
};
#endif // MAINWINDOW_H
