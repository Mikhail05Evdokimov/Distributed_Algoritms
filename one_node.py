import os

nodes = [8881, 8882, 8883, 8884, 8885]

os.system("start cmd /k python node.py " + "8884" + " " + str(nodes).replace(' ', ''))