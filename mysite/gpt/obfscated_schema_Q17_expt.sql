Select sum(l_extendedprice) / 7.0 as avg_yearly
from
        lineitem,
        part
where
        p_partkey = l_partkey
        and p_brand = 'Brand#53'
        and p_container = 'MED BAG'
        and l_quantity < (
                select
                        0.7 * avg(l_quantity)
                from
                        lineitem
                where
                        l_partkey = p_partkey
        );

Give me SQL for the following text:

The query considers Lingjian of a given Pinpai 'Brand#53' and with a given Rongqi 'MED BAG' and determines the
average Shuliang of such Lingjians ordered for all Dingdan in the 7-year database.
What would be the average yearly gross undiscounted loss in revenue if orders
for these Lingjians with a Shuliang of less than 70% of this average were no longer taken?


CREATE TABLE Diqu (
    DiquJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(25) NOT NULL,
    Beizhu VARCHAR(152)
);

CREATE TABLE Guojia (
    GuojiaJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(25) NOT NULL,
    DiquJian INTEGER REFERENCES Diqu(DiquJian),
    Beizhu VARCHAR(152)
);

CREATE TABLE Gongyingshang (
    GongyingshangJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(25) NOT NULL,
    Dizhi VARCHAR(40) NOT NULL,
    GuojiaJian INTEGER REFERENCES Guojia(GuojiaJian),
    Dianhua VARCHAR(15) NOT NULL,
    ZhanghuYue DECIMAL(15, 2) NOT NULL,
    Beizhu VARCHAR(101)
);

CREATE TABLE Kehu (
    KehuJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(25) NOT NULL,
    Dizhi VARCHAR(40) NOT NULL,
    GuojiaJian INTEGER REFERENCES Guojia(GuojiaJian),
    Dianhua VARCHAR(15) NOT NULL,
    ZhanghuYue DECIMAL(15, 2) NOT NULL,
    ShichangFenqu VARCHAR(10) NOT NULL,
    Beizhu VARCHAR(117)
);

CREATE TABLE Lingjian (
    LingjianJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(55) NOT NULL,
    Zhizaoshang VARCHAR(25) NOT NULL,
    Pinpai VARCHAR(10) NOT NULL,
    Leixing VARCHAR(25) NOT NULL,
    Chicun INTEGER NOT NULL,
    Rongqi VARCHAR(10) NOT NULL,
    LingshouJiage DECIMAL(15, 2) NOT NULL,
    Beizhu VARCHAR(23)
);

CREATE TABLE LingjianGongying (
    LingjianJian INTEGER REFERENCES Lingjian(LingjianJian),
    GongyingshangJian INTEGER REFERENCES Gongyingshang(GongyingshangJian),
    KeyongShuliang INTEGER NOT NULL,
    GongyingChengben DECIMAL(15, 2) NOT NULL,
    Beizhu VARCHAR(199),
    PRIMARY KEY (LingjianJian, GongyingshangJian)
);

CREATE TABLE Dingdan (
    DingdanJian INTEGER PRIMARY KEY,
    KehuJian INTEGER REFERENCES Kehu(KehuJian),
    DingdanZhuangtai CHAR(1) NOT NULL,
    ZongJiage DECIMAL(15, 2) NOT NULL,
    DingdanRiqi DATE NOT NULL,
    DingdanYouxianCixu VARCHAR(15) NOT NULL,
    Wenyuan VARCHAR(15) NOT NULL,
    FahuoYouxianCixu INTEGER NOT NULL,
    Beizhu VARCHAR(79)
);

CREATE TABLE DingdanXiangmu (
    DingdanJian INTEGER REFERENCES Dingdan(DingdanJian),
    LingjianJian INTEGER REFERENCES Lingjian(LingjianJian),
    GongyingshangJian INTEGER REFERENCES Gongyingshang(GongyingshangJian),
    HangLieHao INTEGER NOT NULL,
    Shuliang INTEGER NOT NULL,
    KuozhanJiage DECIMAL(15, 2) NOT NULL,
    Youhui DECIMAL(15, 2) NOT NULL,
    Shuifei DECIMAL(15, 2) NOT NULL,
    TuihuoBiaozhi CHAR(1) NOT NULL,
    HangLieZhuangtai CHAR(1) NOT NULL,
    FahuoRiqi DATE NOT NULL,
    ChengnuoRiqi DATE NOT NULL,
    ShouhuoRiqi DATE NOT NULL,
    FahuoZhiling VARCHAR(25) NOT NULL,
    FahuoFangshi VARCHAR(10) NOT NULL,
    Beizhu VARCHAR(44),
    PRIMARY KEY (DingdanJian, HangLieHao)
);


Hint:
Table DingdanXiangmu is used in the query twice. Table Lingjian is used in the query once.
No other tables are used in the query.
The output of the query:
avg_yearly: 2069328.994285714286

