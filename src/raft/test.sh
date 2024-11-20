#!/bin/bash
for ((i = 0; i < 30; i++)) 
do
    echo "= Test $i"
    # Go test command > log
    # 假设 Go 测试命令是 `go test > log`
    go  test -run 3A  > log
    
done

# 查找 log 中的 FAIL 字符串，并计算出现次数
grep "FAIL" log | wc -l
