
import math

for i in range(668):
    table_num = math.ceil((i+1)/100)
    file_num = i + 1
    print('table_num',table_num)
    print('file_num',file_num)

    if i%100 == 0:
        print('creatable')
