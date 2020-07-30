#include "sigfig.h"

sigFig::sigFig(Relation *r):rel(r)
{
    maxRank = 30;
    sigSize = 1;
    highlight = false;
}

// 下面的sigSize是对疏密程度的修正比例
int sigFig::genDotR()
{
    dotR = int(baseDotR + maxDotR * rel->getRank()/maxRank);
    dotR *= sigSize;
    return dotR;
}

int sigFig::genCirWid()
{
    cirWid = baseCirWid + int(maxCirOff * rel->getRank()/maxRank);
    cirWid *= sigSize;
    return cirWid;
}

int sigFig::genTxtWid()
{
    txtWid = baseTxtWid + int(maxTxtOff * rel->getRank()/maxRank);
    txtWid *= sigSize;
    return txtWid;
}

int sigFig::genLineWid(double wei)
{
    return sigSize*(baseLineWid + int(maxLineOff * wei/standardWei));
}

// 一系列的更新和生成函数，生成角度、坐标、宽度等
void sigFig::genDeg(int r, double lasDeg, int lasR)
{
    double lasDegOff = asin(double(lasR)/r);
    double newDegOff = asin(double(dotR)/r);
    deg = lasDeg + lasDegOff + newDegOff;
}

void sigFig::genDotXY(int r, int midX, int midY)
{
    xDot = int(midX + r * cos(deg));
    yDot = int(midY + r * sin(deg));
}

void sigFig::genTxtXY(int r, int midX, int midY)
{
    xTxt = int(midX + r * cos(deg));
    yTxt = int(midY + r * sin(deg));
}

// 汇集所有的生成函数
void sigFig::genAll(int r, int txtOff, double lasDeg, int lasR, double mxRk, int midX, int midY)
{
    setRXY(r,midX,midY);
    setMaxRank(mxRk);
    genCirWid();
    genTxtWid();
    genDotR();
    genDeg(r, lasDeg, lasR);
    genDotXY(r, midX, midY);
    genTxtXY(r + txtOff, midX, midY);
}

// 画点和人名
void sigFig::paintDot(QMainWindow *q)
{
    QPainter pt(q);
//    QPen pen(Qt::darkBlue);

    if (highlight) lineColor = hlColor;
    QPen pen(lineColor);
//    if (highlight) pen.setColor(hlColor);

// 根据计算好的位置和角度，旋转画笔并绘图即可——此时的颜色不需要设置透明度

    pt.save();
    pt.translate(xDot, yDot);
    pen.setWidth(cirWid);
    pt.setPen(pen);
    pt.rotate(getDeg180()-45);
    pt.drawEllipse(0,0,dotR,dotR);
    pt.restore();

    pt.save();
    pt.translate(xTxt, yTxt);
    pt.rotate(getDeg180());
//    pen.setWidth(txtWid);
    QFont font("黑体", txtWid, QFont::Bold, false);
    pt.setFont(font);
    pt.setPen(pen);
    pt.drawText(0,0,rel->getName());
    pt.restore();
}

QPair<int,int> sigFig::calDotXY(double dotDeg)
{
    int xx = int(xMid + cirR * cos(dotDeg));
    int yy = int(yMid + cirR * sin(dotDeg));
    return QPair<int,int>(xx,yy);
}

// 用绘制点的方法，生成1000个点集
QList<QPoint> sigFig::genBesLine(QPoint a, QPoint b, QPoint c)
{
    QList<QPoint> res;
    res.append(a);
    for (double u = 0.001; u<1;u += 0.001)
    {
        QPoint tmpP = u*u*a + 2*u*(1-u)*b + (1-u)*(1-u)*c;
        res.append(tmpP);
    }
    res.append(c);
    return res;
}

int min(int a, int b) {return a>b?b:a;}

// 绘制曲线的函数
// 需要先访问map的角度字典，然后找到目标点的位置
// 以起点、圆心和目标点为基点，下压参数作为调整值，绘制贝塞尔曲线
// 此时的绘制时需要设置透明度，不然结果就没法看了
void sigFig::paintLine(QMainWindow *q, std::map<QString, double> degList, QString limitName)
{
    QPainter pt(q);
//    QPen pen(QColor(32,32,160,lineTransp));
//    QPen pen(lineColor, lineTransp);
    if (highlight) lineColor = hlColor;
    QPen pen(QColor(lineColor.red(),lineColor.green(), lineColor.blue(), lineTransp));
    for (QPair<QString,double>* qp: rel->getReList())
    {
        auto iter = degList.find(qp->first);
//        int limitPos =  min(degList.size(),vitalNum) - 1;
        double limitDeg = degList.find(limitName)->second;
        if (iter != degList.end() && iter->second > deg && iter->second <= limitDeg)
        {
            QPair<int,int> resXY = calDotXY(iter->second);
            QPoint a(xDot,yDot);
            QPoint b(xMid,yMid);
            QPoint c(resXY.first,resXY.second);
            b = (1-lowSize)*(a+c)/2.0 + lowSize*b;
            QList<QPoint> pts = genBesLine(a,b,c);

            pt.save();
            int lineWid = (baseLineWid + maxLineOff * qp->second/standardWei);
            pen.setWidth(lineWid);
            pt.setPen(pen);
            for(QPoint qpt:pts)
            {
                pt.drawPoint(qpt);
            }
            pt.restore();
        }
    }

}
