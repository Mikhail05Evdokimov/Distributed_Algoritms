import os

nodes = [8881, 8882, 8883, 8884, 8885]
#nodes = [8882, 8881, 8883]

for i in nodes:
    os.system("start cmd /k python node.py " + str(i) + " " + str(nodes).replace(' ', ''))
    
