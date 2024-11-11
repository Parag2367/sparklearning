import sys
#import time as t
from time import time as t
#import datetime
from datetime import date #only importing those classes which are necessary not everything like above 

#print (t())

#print(date.today())

# New thing
# use of __name__ global variable

#print("the name of this module", __name__)

# if we run the file directly then value of __name__ is set to main, but if we rhave it indirectly the value will be the name of the module or file

if __name__ == "__main__":
    print("Exceuted when ivoked directly")
else:
    print("Executed when imported")