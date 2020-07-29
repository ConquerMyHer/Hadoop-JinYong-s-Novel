#ifndef SIGFIGLIST_H
#define SIGFIGLIST_H

#include "sigfig.h"
#include <QList>
#include <QPainter>
#include <QMainWindow>
#include <QPen>
#include <map>
#include <QTime>
#include <QDebug>

// 统筹所有的图元类，被mainwindow调用以绘制和调参
// 维护了一个每个图元的角度的map，以使每个图元绘制关系曲线时可以确切地知道它的目标的位置

class sigFigList
{
public:
    sigFigList();

    // 一系列的更新和生成函数
    void genMaxRank();
    void genDegList();
    void genColor();
    void genAll();
    // 设置函数
    void setVitalNum(int vt) {if (vt >= 20 && vt <= 100) vitalNum = vt;}
    void setSigSize(double sz) {sigSize = sz;}
    // 绘制函数
    void paintDot(QMainWindow * q);
    void paintLine(QMainWindow * q);
    void paintAll(QMainWindow * q);

    // 图元组
    QList<sigFig*> sigLs;

private:
    // 宏观上的绘制参数
    const int cirR = 350;
    const int midX = 950;
    const int midY = 500;
    const int txtOff = 70;

    // 显示数量和显示疏密程度
    int vitalNum;
    double sigSize;

    // 一个包含所有可选颜色的颜色表，进一步改进以改变观感
    QList<QColor> allColor;

    double maxRank;

    std::map<QString, double> degList;
    // 所有图元的角度列表
};
#endif // SIGFIGLIST_H
