#include "sigfiglist.h"

sigFigList::sigFigList()
{
    maxRank = 30;
    vitalNum = 30;
    sigSize = 1;
    sigLs = *new QList<sigFig*>();

    // 自定义可用的颜色列表
    allColor.append(QColor(192,0,0));
    allColor.append(QColor(0,192,0));
    allColor.append(QColor(0,0,192));
    allColor.append(QColor(192,64,0));
    allColor.append(QColor(192,0,64));
    allColor.append(QColor(0,192,64));
    allColor.append(QColor(0,64,192));
    allColor.append(QColor(64,192,0));
    allColor.append(QColor(64,0,192));
    allColor.append(QColor(128,0,0));
    allColor.append(QColor(0,128,0));
    allColor.append(QColor(0,0,128));
    allColor.append(QColor(128,128,0));
    allColor.append(QColor(0,128,128));
    allColor.append(QColor(128,0,128));

}

// 原本想搞个随机化，想了想觉得没必要
// 向每个sigFig分发颜色
void sigFigList::genColor()
{
    for(int i = 0; i< sigLs.length();++i)
    {
        //        QTime tm;
        //        tm = QTime::currentTime();
        //        qsrand(tm.msec()+tm.second()*1000);
        //        int rdColor = qrand()%allColor.length();
        //        qDebug() << rdColor << endl;
        sigLs[i]->setLineColor(allColor[i%allColor.length()]);
    }
}

void sigFigList::genMaxRank()
{
    maxRank = sigLs[0]->getRel()->getRank();
}

// 这里如果不清空则会造成目标点无法调整的情况，因为map的性质使然
void sigFigList::genDegList()
{
//    if (!sigLs.isEmpty())
//        sigLs.clear();
    degList.clear();
    for(sigFig * ss: sigLs)
    {
        degList.insert(std::map<QString, double>::value_type(ss->getRel()->getName(), ss->getDeg()));
    }
}

// 必须依次访问sigFig成员并计算角度等参数，才能正常地全部更新
void sigFigList::genAll()
{
    genColor();
    double curDeg = 0;
    int curDotR = 0;

    genMaxRank();
    for(sigFig * s: sigLs)
    {
        s->setSigSize(sigSize);
        s->genAll(cirR, txtOff, curDeg, curDotR, maxRank, midX, midY);
        curDeg = s->getDeg();
        curDotR = s->getDotR();
    }
    genDegList();
}

//
void sigFigList::paintDot(QMainWindow * q)
{
    //    QPainter pt(q);
    //    QPen pen(Qt::darkBlue);
//    genAll();
    int i = 0;
    for(sigFig* s: sigLs)
    {
        if (i < vitalNum)
            s->paintDot(q);
        ++i;
    }
}

// 注意vitalNum对于其绘制的限制
void sigFigList::paintLine(QMainWindow * q)
{
//    genAll();
    int i = 0;
    for(sigFig* s: sigLs)
    {
        if (i < vitalNum)
        {
            int limitPos = vitalNum < sigLs.length()?vitalNum : sigLs.length();
            limitPos -= 1;
            QString limitName = sigLs[limitPos]->getRel()->getName();
            s->paintLine(q,degList,limitName);
        }
        ++i;
    }
}

// 必须要对全部的数据更新后才能再绘制
void sigFigList::paintAll(QMainWindow *q)
{
    genAll();
    paintDot(q);
    paintLine(q);
}
