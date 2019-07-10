import time
import threading

a = 0

def ff():
    global a
    a = 1



def f():
    time.sleep(2)
    global a
    print(a)




threading.Thread(target=f).start()
threading.Thread(target=ff).start()



print('s')
