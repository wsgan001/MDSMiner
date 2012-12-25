# 
# Event
# Class to represent an event. Contains 
# 

import copy
import functools

class Event:
    
    ######################################################################
    # Construction
    ######################################################################
    def __init__(self, eventstr = ""):
        """
        Constructs a new event from the given string. 
        """
        # Extracting the events
        eventstr = eventstr.strip()
        eventstr = eventstr.strip("\n")
        eventinfo= eventstr.split(",")
        if len(eventinfo) < 2:
           self.type = "-1"
           self.time = int(-1)
        else:
           self.type = eventinfo[0]
           self.time = int(eventinfo[1])
        
        self.__len__ = 1
        
	    # Creating the representation
        self.createRepr()
        
    
    ######################################################################
    # Getting
    ######################################################################
    def getType(self):
        """
        Returns the type of the event
        """
        return self.type
    
    def getTime(self):
        """
        Returns the occur time of the event
        """
        return self.time
    
    ######################################################################
    # Object related
    ######################################################################
    def createRepr(self):
        """
        Makes a representation of the event. 
        """
        self.repr = "<"
        self.repr += "%s,%s"%(self.type,self.time)
        self.repr += ">"
        
    
    def __repr__(self):
        """
        Returns the representation of this event
        """
        return self.repr
    
    
    def __eq__(self, other):
        """
        Are two event equal?
        """
        if (other != None):
            return self.repr == other.repr
        else:
            return False
    
    
    def __bf__(self, other):
        """
        Tests event is before than other or not 
        """
        if(other != None and self.time <= other.time):
            return True
        else:
            return False

    def __af__(self, other):
        """
        Tests event is after than other or not 
        """
        return other.__bf__(self)
    
    def __st__(self, other):
        """
        Tests event has same type with other 
        """
        if(other != None and self.type == other.type):
            return True
        else:
            return False
    
    def __hash__(self):
        """
        Hashes this event
        """
        return self.repr.__hash__()
    
    
    
    
    
