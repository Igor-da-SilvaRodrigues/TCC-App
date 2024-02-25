import sys
import numpy as np

#returns False if the number is an outlier
def validate(number, q1, q3):
    iqr = q3 - q1
    return False if number < q1-1.5*iqr or number > q3+1.5*iqr else True


def main(args:list):
    valueList = [float(i) for i in args]
    sortedList = sorted(valueList)
    
    q1,q3 = np.percentile(sortedList, [25,75])
    print([i for i in sortedList if validate(i, q1, q3)])

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("No arguments")
    else:    
        main(sys.argv[1:len(sys.argv)]) #pass only the value arguments