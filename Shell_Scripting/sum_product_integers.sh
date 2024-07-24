#! /bin/bash

echo -n "Enter an integer: "
read n1

echo -n "Enter another integer: "
read n2

sum=$(($n1+$n2))
product=$(($n1*$n2))

echo "The sum of $n1 and $n2 is $sum"
echo "The product of $n1 and $n2 is $product."

if [[ $sum == $product ]]
then
    echo "The sum and product are equal"
elif [ $sum -lt $product ]
then
    echo "The sum is less than product"
else
    echo "The product is less than sum"
fi