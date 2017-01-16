## Kaggle Coupon Purchase Prediction Competition

My implementation for Kaggle's [Coupon Purchase Prediction](https://www.kaggle.com/c/coupon-purchase-prediction) (*Recruit Ponpare* Challenge).

The code is written in Scala and Spark and is my attempt at applying what I learned from Sandy Ryza's [Advanced Analytics with Spark](http://shop.oreilly.com/product/0636920035091.do).

### Requirements

- **Spark**: 1.2.1 and up
- **Scala**: 2.10.4 and up

### Data

Place the [data files](https://www.kaggle.com/c/coupon-purchase-prediction/data) into a subfolder `./Coupon_Purchase_Prediction/data`. 

Data includes:

- `coupon_list_train.csv` 
- `user_list.csv`
- `coupon_detail_train.csv`
- `coupon_list_test.csv` 

### Run

    $ SPARKHOME/bin/spark-shell -i train.scala
