#include "mainwindow.h"

#include <QApplication>

/*
*   本程序提供对输入关系表进行可视化的操作
*   输入的格式为 单行：人物 \t PageRank \t 人物 \t 关系权重 \t ...
*   且需要对PageRank进行降序排列
*
*   在程序的输出界面中，你可以拖动界面左侧的两个滚动条，
*   你将调整可视化的人物的数量以及其排列的疏密程度
*   其中，越是紧密的关系将用越是粗厚的线条显示
*   而pagerank越高的人物将用越粗的字体和空心圆表示
*
*   本项目由于时间关系，只完成了主要逻辑，其余细节的调整均尚未完善
*   因此鲁棒性延拓性极差，请保证输入数据的正确性，否则程序很可能崩溃导致意外结果。
*
*   RelationPainter v0.0.1
*   韩畅 2020/7/29
*/


int main(int argc, char *argv[])
{
    QApplication a(argc, argv);
    MainWindow w;
    w.show();
    return a.exec();
}
