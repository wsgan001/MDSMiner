# 
# Window
# A window is an ordered list of events. 
# 

import copy
import functools

class Window(list):
    
   
    def __init__(self, length):
        """
        Constructor
        """
        # Calling the super
        super(Window, self).__init__()
        
        # Storing values
        self.length     = length
        self.starttime = 0
        self.endtime = self.starttime
        self.streamEventTypes = {}
    
    ######################################################################
    # Management
    ######################################################################
    def append(self, event):
        """
        Adds an element to the window, and returns the element that 
        drops out of the window. 
        """
        # Adding as the first element
        if len(self) == 0:
            self.starttime = event.time
        self.endtime = event.time
        self.insert(0, event)
       
        if self.streamEventTypes.has_key(event.type):
            self.streamEventTypes[event.type] += 1
        else:
            self.streamEventTypes[event.type] = 1

        # If window is overfull, remove the oldest (first) element
        # Or if the window is of length 1
        out = None
        #if self.length < len(self) or self.length == 1:
        if self.length < len(self):
            out = self[-1]
            del self[-1]
            self.starttime = self[-1].time
        
        return out
        
    def filled(self):
        """
        Returns whether the window is full (i.e. contains length 
        elements). 
        """
        return len(self) == self.length
    
    
    
    ######################################################################
    # Frequency
    ######################################################################
    def getCount(self, event):
        """
        Returns the count of the given event in this window. 
        """
        # Attempt 2: Still too slow
        count = 0
        
        for mEvent in self:
            if event.__st__(mEvent):
                count += 1
        
        return count
        
        # Attempt 1: Too slow
        #return reduce((lambda x, y: x+y),
        #            map((lambda i: itemset <= i), self))
    
    
    ######################################################################
    # Getters
    ######################################################################
    def getStreamEventTypes(self):
        return self.streamEventTypes
    
    
    
