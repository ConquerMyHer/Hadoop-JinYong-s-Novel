#include "mainwindow.h"
#include "ui_mainwindow.h"

MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent)
    , ui(new Ui::MainWindow)
{
    ui->setupUi(this);
    // 尚未装载文件
    loaded = false;
    curHlInd = 0;

    // 对鼠标事件的信号槽连接
    connect(this,SIGNAL(mouseMove(QMouseEvent *)),this,SLOT(myMouseMoveHandler(QMouseEvent *)));

    QMainWindow::setMouseTracking(true);

    QMainWindow::centralWidget()->setMouseTracking(true);

    ui->centralwidget->setMouseTracking(true);
}

MainWindow::~MainWindow()
{
    delete ui;
}

void MainWindow::loadFile()
{
    //    QTextCodec *codec = QTextCodec::codecForName("GBK");

    //装载文件，存入fgLs的sigLs中
    QString fileName = QFileDialog::getOpenFileName(this, tr("Open File"),
                                                    QDir::currentPath(), tr("File (*.txt)"));

    if (!fileName.isEmpty())
    {
        QFile * f = new QFile(fileName);
        if (!f->open(QIODevice::ReadOnly | QIODevice::Text))
        {
            qDebug()<<"can't open the file!" << endl;
            return;
        }// 对文件可读性的简单判断
        else
        {
            // 逐行拆解
            while(!f->atEnd())
            {
                QByteArray line = f->readLine();

                QString str(line);
                QStringList strLs(str.split("\t",QString::SkipEmptyParts));
                // 以\t为分隔符
                if (strLs.length()>=2)
                {
                    //                    reLs.append(new Relation(strLs[0], strLs[1].toDouble()));
                    fgLs.sigLs.append(new sigFig(new Relation(strLs[0], strLs[1].toDouble())));
                    for(int i = 1; 2*i+1 < strLs.length();++i)
                    {
                        //对关系表进行填充
                        QString relaName(strLs[2*i]);
                        double relaWei(strLs[2*i+1].toDouble());
                        (*(fgLs.sigLs.end()-1))->getRel()->addNewRe(relaName,relaWei);
                    }
                }
            }
            // 已装载设置为true（其实是一开始的做法，现在可以用是否为空的判断代替）
            loaded = true;
            //            fgLs.genAll();
        }
    }
}

// 生成新图元的角度 目前失效，挪到了sigfig里
//double genNewDegree(double lasDeg, int lasR, int newR, int r)
//{
//    double lasDegOff = asin(double(lasR)/r);
//    double newDegOff = asin(double(newR)/r);
//    return lasDeg + lasDegOff + newDegOff;
//}

//QPair<int,int> genDotCenPos(int r, double degree, int midX, int midY)
//{
//    int x = int(midX + r * cos(degree));
//    int y = int(midY + r * sin(degree));
//    return QPair<int,int>(x,y);
//}

void MainWindow::paintEvent(QPaintEvent *)
{
    if (loaded)
    {
        // 直接调用fgLs的绘制函数绘制全部图元
        //        fgLs.paintDot(this);
        //        fgLs.paintLine(this);
        fgLs.paintAll(this);
    }
}

// 点击转载文件时的响应
void MainWindow::on_FileBrowse_clicked()
{
    loadFile();
}

// 两个滚动条的响应
void MainWindow::on_vitalNumSlider_valueChanged(int value)
{
    fgLs.setVitalNum(ui->vitalNumSlider->value());
    this->repaint();
}

void MainWindow::on_sigSizeSlider_valueChanged(int value)
{
    fgLs.setSigSize(ui->sigSizeSlider->value()/50.0);
    this->repaint();
}

// 鼠标活动的信号
void MainWindow::mouseMoveEvent(QMouseEvent *event)
{
    emit mouseMove(event);
}

//// 简易的的距离函数
//double disTo(int x1,int y1,int x2,int y2)
//{
//    return sqrt((x1-x2)*(x1-x2)+(y1-y2)*(y1-y2));
//}

// 对鼠标移动的位置进行是否需要高亮或取消高亮的判断
// 选取距离鼠标位置最近的人物及其关系进行高亮显示
void MainWindow::myMouseMoveHandler(QMouseEvent * e)
{
    int curX = e->x();
    int curY = e->y();
    //    qDebug() << "curX : " << curX << ", curY : " << curY << endl;

    int curMinDis = 100000;
    int curMinDisInd = 0;

    for (int i = 0; i < fgLs.getVitalNum() && i < fgLs.sigLs.length(); ++i)
    {
        sigFig * sf = fgLs.sigLs[i];
        int dotX = sf->getXDot();
        int dotY = sf->getYDot();
        int curDis = abs(curX-dotX) + abs(curY-dotY);
//        int curDis = disTo(curX,curY,dotX,dotY);
        if (curDis < curMinDis)
        {
            fgLs.sigLs[curMinDisInd]->setHighLight(false);
            curMinDisInd = i;
            curMinDis = curDis;
        }
        else
        {
            fgLs.sigLs[i]->setHighLight(false);
        }

    }
    if (curMinDisInd != curHlInd)
    {
        fgLs.sigLs[curMinDisInd]->setHighLight(true);
        this->repaint();
        curHlInd = curMinDisInd;
    }

}

void MainWindow::on_transpSlider_valueChanged(int value)
{
    fgLs.setTrans(ui->transpSlider->value());
    this->repaint();
}
