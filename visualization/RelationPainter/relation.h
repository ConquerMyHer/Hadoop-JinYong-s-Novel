#ifndef RELATIONS_H
#define RELATIONS_H

#include <QString>
#include <QList>
#include <QPair>

// 这是一个刻画单行人物关系的类
class Relation
{
public:
    Relation(QString n, double r);
    // 添加新的关系
    void addNewRe(QString n, double w);

    //获取函数及简单的参数判断函数
    QString getName(){return name;}
    double getRank(){return rank;}
    QList<QPair<QString,double>*> getReList(){return reList;}

    int getListLen(){return reList.length();}
    bool isEmpty(){return reList.isEmpty();}
private:
    QString name; // 人物姓名
    double rank;// 人物的pageRank
    QList<QPair<QString,double>*> reList; // 与其他人的关系表
};

#endif // RELATIONS_H
