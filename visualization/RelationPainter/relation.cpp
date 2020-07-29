#include "relation.h"

Relation::Relation(QString n, double r)
{
    name = n;
    rank = r;
}

// 添加新关系对到列表
void Relation::addNewRe(QString n, double w)
{
    reList.append(new QPair<QString,double>(n,w));
}
