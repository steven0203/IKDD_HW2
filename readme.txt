myudfs 是我的udf
myudfs.jar 是udf的jar
test2.pig 是pig檔
0930.csv 是input
date.txt 是日期的input ,格式 xxxx/xx/xx,xxxx/xx/xx ,第一個是起始日期,第二個是結束日期


執行 ：pig -x local -param distance=數字  -param top_k=數字 test2.pig
top_k 筆資料中若第k筆的count和後面排名重複,會將同樣count的一起輸出


ex:

執行pig -x local -param distance=2000.0  -param top_k=4 test2.pig
輸出
(5,6)
(2,5)
(3,5)
(4,5)
(6,5)
(7,5)

因為第4筆和後面幾筆的count一樣,所以後幾筆也輸出

