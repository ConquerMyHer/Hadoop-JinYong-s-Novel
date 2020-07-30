#ifndef SIGFIG_H
#define SIGFIG_H

#include "relation.h"
#include <math.h>
#include <QMainWindow>
#include <QPainter>
#include <QPen>
#include <QFont>
#include <QColor>
#include <map>
#include <iterator>
#include <QPoint>

#define PI 3.1415926

// 以单行关系表为基础的单图元组类
class sigFig
{
public:
    sigFig(Relation * r);

    // 关系表无论如何是需要被访问的
    Relation * getRel(){return rel;}

    // 一系列的更新函数
    void setMaxRank(double m) {maxRank = m;} // 通过更新MaxRank来确定Dot的粗细
    void genDeg(int r, double lasDeg, int lasR);
    QPair<int,int> calDotXY(double dotDeg);
    void setRXY(int r, int midX, int midY){cirR = r; xMid = midX; yMid = midY;}
    void genDotXY(int r, int midX, int midY);
    void genTxtXY(int r, int midX, int midY);
    void genAll(int r, int txtOff, double lasDeg, int lasR, double mxRk, int midX, int midY);
    // 最终由一个总更新函数汇总

    // 一系列的设置函数
    void setLineColor(QColor cl){lineColor = cl;}
    void setSigSize(double sSz){sigSize = sSz;}
    void setHighLight(bool h){highlight = h;}
    bool getHighLight(){return highlight;}
    void setTranp(int tr){lineTransp = tr;}

    // 一系列的获取函数
    int getXDot() {return xDot;}
    int getYDot() {return yDot;}
    int getXTxt() {return xTxt;}
    int getYTxt() {return yTxt;}

    double getDeg() {return deg;}
    //    double getDeg180() {return (deg<PI*1/2.0||deg>PI*3/2.0)?deg*180/PI:(deg-PI)*180/PI;}
    double getDeg180() {return deg*180/PI;}
    int getDotR() {return dotR;}
    int getCirWid() {return cirWid;}
    int getTxtWid() {return txtWid;}

    // 最重要的图元绘制函数
    void paintDot(QMainWindow * q);
    void paintLine(QMainWindow * q, std::map<QString, double> degList, QString limitName);
private:
    Relation * rel;
    double deg;// 0-2PI
    int dotR;// 小点的半径
    int cirWid;// 小点的粗细
    int txtWid;// 文字的粗细
    //    int lineWid;

    double maxRank;// 最大的rank值
    double sigSize;// 由于疏密程度而对图元的修正值
    int lineTransp;// 透明度

    bool highlight;// 是否需要高亮

    // 一系列的图元绘制常量
    const int baseDotR = 5;// 基础的Dot的半径
    const int maxDotR = 40;// 最大的Dot半径的增量
    const int baseCirWid = 5;// 下面的类似上面两条
    const int maxCirOff = 10;
    const int baseTxtWid = 7;
    const int maxTxtOff = 15;

    const double standardWei = 0.1;
    const int baseLineWid = 1;
    const int maxLineOff = 10;
    const double lowSize = 0.5;// 将曲线压低的比例（避免一团乱麻）0-1
    const QColor hlColor = QColor(0,0,0);// 高亮颜色

    QColor lineColor;// 本色

    // 各种坐标

    int xDot;
    int yDot;
    int xTxt;
    int yTxt;

    int xMid;
    int yMid;
    int cirR;

    // 一系列私有的更新函数
    int genCirWid();
    int genTxtWid();
    int genLineWid(double wei);
    int genDotR();

    // 计算一个简单的贝塞尔曲线点集（估计这里的处理效率会很低，以后有机会一定会改）
    QList<QPoint> genBesLine(QPoint a, QPoint b, QPoint c);
};

#endif // SIGFIG_H
